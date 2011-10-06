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
  where companies.ticker = 'MSFT'
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

  # test
  classifier[:docs][:test].each do |doc|
    if (classifier[:bayes].classify doc['body']) == 'Buy'
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

