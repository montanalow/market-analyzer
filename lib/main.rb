require 'dm-core'
DataMapper.setup(:default, 'mysql://localhost/mercury')
class Corporation
  include DataMapper::Resource

  property :id, Serial
  property :ticker, String
  property :name, String
  property :sec_filings, String
  property :gics_sector, String
  property :headquarters, String
end
DataMapper.finalize



Encoding.default_internal = 'utf-8'
Encoding.default_external = 'utf-8'

require 'net/http'
require 'net/https'
require 'date'
require 'cgi'
require 'nokogiri'

require 'mysql'
@mysql = Mysql.new '127.0.0.1', 'root', nil, 'mercury', 3306
@mysql.reconnect = true
@mysql.query "set names utf8"

require 'httpclient'
@client = HTTPClient.new
@client.redirect_uri_callback = Proc.new do |uri, res|
   newuri = URI.parse(res.header['location'][0])
   unless newuri.is_a?(URI::HTTP)
     newuri = uri + newuri
     STDERR.puts("could be a relative URI in location header which is not recommended")
     STDERR.puts("'The field value consists of a single absolute URI' in HTTP spec")
   end
   # dumbasses at wsj don't do ssl for login urls and then redirect incorrectly from https back to http
#   if https?(uri) && !https?(newuri)
#     raise BadResponseError.new("redirecting to non-https resource")
#   end
   puts "redirect to: #{newuri}" if $DEBUG
   newuri
end
@client.post_content 'https://commerce.wsj.com/auth/submitlogin', 'user' => 'montanalow@gmail.com', 'password' => 'kayo5blur', 'submit' => 'log in'

# fetch document sources
corporations = @mysql.query "
  select id, ticker
  from corporations
  inner join market
  on market.corporation_id = corporations.id
  group by id
  order by sum(volume * closing) DESC
"
corporations.num_rows.times do
  corporation = corporations.fetch_hash
  page = 0
  loop do
    sleep 10
    url = "http://online.wsj.com/quotes/news.html?nextPageNumber=#{page += 1}&symbol=#{corporation['ticker']}&type=usstock"
    puts "fetching #{url}"
    doc = Nokogiri::HTML @client.get_content url
    trs = (doc.xpath '//div[@id="quoteBody"]/div[4]/table/tr')
    if trs.size > 0
      trs.each do |tr|
        date, url = tr.xpath 'td'
        begin
          date = (Time.parse date.content).to_date
          url = (url.xpath 'a')[0]
          title = url.content.strip
          url = url['href']
          url = "http://online.wsj.com" + url unless url.start_with? "http"
          puts "inserting '#{date}', '#{title}', '#{url}'"
          begin
            @mysql.query "insert into documents (date, source, title, state) values ('#{Mysql.escape_string date.to_s}', '#{Mysql.escape_string url}', '#{Mysql.escape_string title}', 'new')"
            @mysql.query "insert into corporations_documents values (#{corporation['id']},#{@mysql.insert_id})"
          rescue Mysql::Error => ex
            puts "*** dup '#{date}', '#{title}', '#{url}'"
            document = @mysql.query "select id from documents where source = '#{Mysql.escape_string url}'"
            @mysql.query "insert ignore into corporations_documents values (#{corporation['id']},#{document.fetch_row[0]})"
          end
        rescue ArgumentError, NoMethodError => ex
          puts "#{ex}"
          break
        end
      end
    else
      break
    end
  end
end

# fetch documents
loop do
  documents = @mysql.query "select id, source from documents where state = 'new' limit 1"
  break if documents.num_rows < 1
  documents.num_rows.times do
    document = documents.fetch_hash
    puts "new #{document['source']}"
    retries = 2
    begin
      response = @client.get_content document['source']
      @mysql.query "
        update documents
        set raw = '#{Mysql.escape_string response}',
        state = 'fetched'
        where id = #{document['id']}
      "
      puts "fetched #{document['source']}"
    rescue HTTPClient::BadResponseError => ex
      if retries > 0
        retries -= 1
        puts "oops retry"
        retry
      else
        @mysql.query "
          update documents
          set state = 'bad source'
          where id = #{document['id']}
        "
        puts "fail bag"
      end
    end
  end
end

# process documents
loop do
  documents = @mysql.query "select id, raw from documents where state = 'fetched' limit 1"
  break if documents.num_rows < 1
  documents.num_rows.times do
    document = documents.fetch_hash
    body = (((Nokogiri::HTML document['raw']).xpath "//div[@class='articlePage']").map{|p| p.text}.join '').strip
    if body.size > 0
      @mysql.query "
        update documents
        set body = '#{Mysql.escape_string body}',
        state = 'processed'
        where id = #{document['id']}
      "
      puts "did #{document['id']}"
    else
      @mysql.query "
        update documents
        set body = NULL,
        state = 'error processing'
        where id = #{document['id']}
      "
      puts "failed #{document['id']}"
    end
  end
end


def doc id
  documents = @mysql.query "select * from documents where id = #{id.to_i} limit 1"
  if documents.num_rows > 0
    documents.fetch_hash
  else
    nil
  end
end



# stock data
require 'httpclient'
require 'csv'
require 'mysql'
require 'date'
companies = @mysql.query "
  select id, ticker
  from companies
  inner join market
  on market.company_id = companies.id
  group by id
  order by sum(volume * close) DESC
"
companies.each_hash do |company|
  begin
    url = "http://ichart.finance.yahoo.com/table.csv?s=#{company['ticker']}&a=01&b=01&c=1900&d=01&e=01&f=2012&g=d&ignore=.csv"
    puts "fetching #{url}"
    csv = CSV.parse (HTTPClient.new.get_content url), :headers => true
    @mysql.query "
      insert into market (company_id, date, open, close, high, low, volume)
      values #{csv.map{|line| "(#{company['id']}, '#{Mysql.escape_string line['Date']}', #{(line['Open'])}, #{(line['Close'])}, #{(line['High'])}, #{(line['Low'])}, #{line['Volume']})"}.join ','}
      on duplicate key update
        open = values(open),
        close = values(close),
        high = values(high),
        low = values(low),
        volume = values(volume)
    "
    puts @mysql.affected_rows
  rescue HTTPClient::BadResponseError => ex
    puts ex.to_s
  end
end








# next day
classifiers = Array.new(size) do |i|
  {
    :bayes => Classifier::Bayes.new('Buy', 'Sell'),
    :results => {
      :accuracy => 0,
      :positive_accuracy => 0,
      :negative_accuracy => 0,
      :true_positives => 0,
      :false_positives => 0,
      :true_negatives => 0,
      :false_negatives => 0
    },
    :docs => {
      :test => [],
      :train => []
    }
  }
end;nil

# sort docs into test and training sets
classifiers.each_with_index do |classifier,i|
  docs.each do |doc|
    if Date.parse(doc['current_market']).day % size == i
      classifier[:docs][:test] << doc
    else
      classifier[:docs][:train] << doc
    end
  end
end;nil

classifiers.each do |classifier|
  # train
  classifier[:docs][:train].each do |doc|
    if doc['current_closing'].to_i >= doc['current_opening'].to_i
      classifier[:bayes].train_buy doc['next_market']
    else
      classifier[:bayes].train_sell doc['next_market']
    end
  end

  # test
  classifier[:docs][:test].each do |doc|
    if (classifier[:bayes].classify doc['current_market']) == 'Buy'
      if doc['current_closing'].to_i >= doc['current_opening'].to_i
        classifier[:results][:true_positives] += 1
      else
        classifier[:results][:false_positives] += 1
      end
    else
      if doc['current_closing'].to_i <= doc['current_opening'].to_i
        classifier[:results][:true_negatives] += 1
      else
        classifier[:results][:false_negatives] += 1
      end
    end
  end

  classifier[:results][:positive_accuracy] = classifier[:results][:true_positives].to_f / (classifier[:results][:true_positives] + classifier[:results][:false_positives])
  classifier[:results][:negative_accuracy] = classifier[:results][:true_negatives].to_f / (classifier[:results][:true_negatives] + classifier[:results][:false_negatives])
  classifier[:results][:accuracy] = (classifier[:results][:true_positives].to_f + classifier[:results][:true_negatives].to_f) / (classifier[:results][:true_positives] + classifier[:results][:false_positives] + classifier[:results][:true_negatives] + classifier[:results][:false_negatives])
  puts classifier[:results].inspect
end;nil

puts classifiers.map{|classifier| classifier[:results][:accuracy]}.sum / size









# bayes
results = @mysql.query "
  select
    companies.ticker,
    documents.id,
    documents.date as document_published,
    current_market.date as current_market,
    next_market.date as next_market,
    documents.body,
    current_market.opening as current_opening,
    current_market.closing as current_closing,
    next_market.opening as next_opening,
    next_market.closing as next_closing
  from companies_documents
  inner join companies
    on companies.id = companies_documents.company_id
  inner join documents
    on documents.id = companies_documents.document_id
  inner join market current_market
    on current_market.company_id = companies.id
    and current_market.date = (
      select max(date)
      from market
      where market.date <= documents.date
        and market.company_id = companies.id
    )
  inner join market next_market
    on next_market.company_id = companies.id
    and next_market.date = (
      select min(date)
      from market
      where market.date > documents.date
        and market.company_id = companies.id
    )
  where companies.ticker = 'aapl'
    and length(documents.body) > 0
    and documents.state = 'processed'
  order by documents.date

"
docs = []
results.each_hash do |result|
  docs << result
end

require 'classifier'
size = 5
classifiers = Array.new(size) do |i|
  {
    :bayes => Classifier::Bayes.new('Buy', 'Sell'),
    :votes => Hash.new{|hash,key| hash[key] = {:buys => 0, :sells => 0}},
    :results => {
      :accuracy => 0,
      :abstain => 0,
      :positive_accuracy => 0,
      :negative_accuracy => 0,
      :true_positives => 0,
      :false_positives => 0,
      :true_negatives => 0,
      :false_negatives => 0
    },
    :docs => {
      :test => [],
      :train => []
    }
  }
end;nil

classifiers.each_with_index do |classifier,i|
  # sort docs into test and training sets
  docs.each do |doc|
    if Date.parse(doc['current_market']).day % size == i
      classifier[:docs][:test] << doc
    else
      classifier[:docs][:train] << doc
    end
  end
end;nil

# same day
classifiers.each do |classifier|
  # train
  classifier[:docs][:train].each do |doc|
    if doc['current_closing'].to_i >= doc['current_opening'].to_i
      classifier[:bayes].train_buy doc['body']
    else
      classifier[:bayes].train_sell doc['body']
    end
  end


  # vote
  classifier[:docs][:test].each do |doc|
    if (classifier[:bayes].classify doc['body']) == 'Buy'
      classifier[:votes][doc['current_market']][:buys] += 1
    else
      classifier[:votes][doc['current_market']][:sells] += 1
    end
    classifier[:votes][doc['current_market']][:open] = doc['current_opening'].to_f / 100
    classifier[:votes][doc['current_market']][:close] = doc['current_closing'].to_f / 100
    classifier[:votes][doc['current_market']][:delta] = doc['current_closing'].to_f - doc['current_opening'].to_f
  end

  # test
  classifier[:votes].each do |date,vote|
    puts [date,vote[:buys],vote[:sells],vote[:open],vote[:close]].join "\t"
    if vote[:buys] == vote[:sells]
      classifier[:results][:abstain] += 1
    elsif
      vote[:buys] > vote[:sells]
      if vote[:delta] >= 0
        classifier[:results][:true_positives] += 1
      else
        classifier[:results][:false_positives] += 1
      end
    else
      if vote[:delta] <= 0
        classifier[:results][:true_negatives] += 1
      else
        classifier[:results][:false_negatives] += 1
      end
    end
  end


  classifier[:results][:positive_accuracy] = classifier[:results][:true_positives].to_f / (classifier[:results][:true_positives] + classifier[:results][:false_positives])
  classifier[:results][:negative_accuracy] = classifier[:results][:true_negatives].to_f / (classifier[:results][:true_negatives] + classifier[:results][:false_negatives])
  classifier[:results][:accuracy] = (classifier[:results][:true_positives].to_f + classifier[:results][:true_negatives].to_f) / (classifier[:results][:true_positives] + classifier[:results][:false_positives] + classifier[:results][:true_negatives] + classifier[:results][:false_negatives])
  puts classifier[:results].inspect
end;nil

puts classifiers.map{|classifier| classifier[:results][:accuracy]}.sum / size

