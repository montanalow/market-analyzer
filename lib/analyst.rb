require 'mysql'

class Analyst
  def initialize ticker
    @mysql = Mysql.new '127.0.0.1', 'root', nil, 'mercury', 3306
    @mysql.reconnect = true
    @mysql.query "set names utf8"
    @ticker = ticker
    self
  end

  def up? date
    prediction > 0
  end

  def down? date
    prediction < 0
  end
video.match(/Codec ID: (.*)/)[1].include?("MPEG4") && audio.match(/Codec ID: (.*)/)[1].include?("AC3")
  def stable? date
    prediction == 0
  end

  def prediction date
    0
  end

  def test
  end
end

class YesterdaysOptimisticAnalyst < Analyst
  def initialize ticker
    super
    results = @mysql.query "
      select date, open, close
      from market
      inner join companies
        on companies.id = market.company_id
        and companies.ticker = '#{Mysql.escape_string ticker}'
    "
    raise "ticker is not valid" if results.num_rows == 0
    @dates = Hash.new{|hash,key| hash[key] = {}}
    results.each_hash do |result|
      date = Date.parse result['date']
      @dates[date].merge :date => date, :open => result['open'].to_f, :close => result['close'].to_f
      @dates[date + 1] = {:previous => date}
    end
    self
  end

  def prediction date
    yesterday = @dates[@dates[date][:previous]]
    if yesterday
      (yesterday[:close] - yesterday[:open]) / yesterday[:open]
    else
      super
    end
  end

  def test
    results = {
      :accuracy => 0,
      :abstain => 0,
      :positive_accuracy => 0,
      :negative_accuracy => 0,
      :true_positives => 0,
      :false_positives => 0,
      :true_negatives => 0,
      :false_negatives => 0
    }
    @dates.each do |date, values|
      prediction = prediction date
      outcome = (date[:close] - date[:open] / date[:open])
      if prediction > 0
        if outcome >= 0
          results[:true_positives] += 1
        else
          results[:false_positives] += 1
        end
      else
        if outcome <= 0
          results[:true_negatives] += 1
        else
          results[:false_negatives] += 1
        end
      end
      results[:error] += (outcome - prediction).abs
    end
  end
end

y = YesterdaysOptimisticAnalyst.new 'aapl'
y.test
