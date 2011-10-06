create database mercury;
use mercury;

create table companies (
  id int unsigned not null AUTO_INCREMENT,
  ticker char(4) not null,
  name varchar(64) not null,
  sec_filings varchar(64) not null,
  gics_sector varchar(64) not null,
  headquarters varchar(64) not null,
  primary key(id),
  unique key (ticker)
) engine=innodb character set utf8 collate utf8_general_ci;

load data infile '/tmp/corporations.csv'
into table corporations
character set utf8
fields
  terminated by ','
  optionally enclosed by '"'
ignore 1 lines
(ticker,name,sec_filings,gics_sector,headquarters);

create temporary table temp_market (
  ticker char(4) not null,
  date date not null,
  open decimal(10,2) not null,
  close decimal(10,2) not null,
  high decimal(10,2) not null,
  low decimal(10,2) not null,
  volume int unsigned not null,
  primary key(ticker, date)
) engine=memory character set utf8 collate utf8_general_ci;

load data infile '/tmp/market.csv'
into table temp_market
character set utf8
fields
  terminated by ','
  optionally enclosed by '"'
ignore 1 lines
(date,ticker,open,high,low,close,volume);

create table market (
  company_id int unsigned not null,
  date date not null,
  open decimal(10,2) not null,
  close decimal(10,2) not null,
  high decimal(10,2) not null,
  low decimal(10,2) not null,
  volume int unsigned not null,
  primary key (corporation_id, date)
) engine=innodb character set utf8 collate utf8_general_ci;

insert into market
select corporations.id, date, open, close, high, low, volume
from temp_market
inner join corporations
on binary corporations.ticker = binary temp_market.ticker;

drop table temp_market;

create table documents (
  id int unsigned not null AUTO_INCREMENT,
  date date not null,
  title varchar(255),
  source varchar(255),
  raw text,
  body text,
  state varchar(16),
  primary key(id),
  unique key (source)
) engine=innodb character set utf8 collate utf8_general_ci;

create table corporations_documents (
  corporation_id int unsigned not null,
  document_id int unsigned not null,
  primary key (corporation_id, document_id)
) engine=innodb character set utf8 collate utf8_general_ci;


select *
  from corporations_documents
  inner join corporations
    on corporations.id = corporations_documents.corporation_id
  inner join documents
    on corporations_documents.document_id
  inner join market today
    on today.corporation_id = corporations.id
--    and today.date = documents.date
  where corporations.ticker = 'AAPL'

  select documents.body, tomorrow.closing - today.closing
  from corporations_documents
  inner join corporations
    on corporations.id = corporations_documents.corporation_id
  inner join documents
    on corporations_documents.document_id
  inner join market today
    on today.corporation_id = corporations.id
    and today.date = documents.date
  inner join market tomorrow
    on tomorrow.corporation_id = corporations.id
    and tomorrow.date = documents.date + 1
  where corporations.ticker = 'AAPL'
    and length(documents.body) > 0
    and documents.state = 'fetched'

explain
  select companies.ticker, documents.date, documents.body, today.closing, tomorrow.opening, tomorrow.closing
  from companies_documents
  inner join companies
    on companies.id = companies_documents.company_id
  inner join documents
    on companies_documents.document_id = companies_documents.document_id
  inner join market today
    on today.company_id = companies.id
    and today.date = documents.date
  inner join market tomorrow
    on tomorrow.company_id = companies.id
    and tomorrow.date = DATE(documents.date + 1)
  where companies.ticker = 'AAPL'
    and length(documents.body) > 0
    and documents.state = 'processed'
limit 2

  select companies.id, _documents.id
  from companies_documents
  inner join companies
    on companies.id = companies_documents.company_id
  inner join documnets
  where companies.ticker = 'AAPL'


explain
select
  companies.ticker,
  documents.id,
  documents.date as document_published,
  current_market.date as current_market,
  next_market.date as next_market,
--  documents.body,
  current_market.closing as previous_closing,
  next_market.opening as next_opening,
  next_market.closing as next_closing
from companies_documents
inner join companies
  on companies.id = companies_documents.company_id
inner join documents
  on documents.id = companies_documents.document_id
left outer join market current_market
  on current_market.company_id = companies.id
  and current_market.date = (
    select max(date)
    from market
    where market.date <= documents.date
      and market.company_id = companies.id
  )
left outer join market next_market
  on next_market.company_id = companies.id
  and next_market.date = (
    select min(date)
    from market
    where market.date > documents.date
      and market.company_id = companies.id
  )
where companies.ticker = 'AAPL'
  and length(documents.body) > 0
  and documents.state = 'processed'
order by documents.date
limit 20

select id, ticker, name, count(*) from companies_documents inner join companies on companies.id = companies_documents.company_id group by companies.id order by count(*) desc limit 10;



  select
    companies.ticker,
    sum(if(opening < closing,1,0)) as ups,
    sum(if(opening > closing,1,0)) as downs,
    sum(if(opening < closing,1,0)) / (sum(if(opening < closing,1,0)) + sum(if(opening > closing,1,0))) as percent
  from companies
  inner join market
    on market.company_id = companies.id
    and market.date between '2011-01-17' and '2011-02-18'
  where companies.ticker in ('goog','aapl','msft')
  group by companies.ticker


  select
    companies.ticker,
    opening.opening,
    closing.closing,
    (CAST(closing.closing AS DECIMAL) - CAST(opening.opening AS DECIMAL)) / opening.opening * 100 as percent
  from companies
  inner join market opening
    on opening.company_id = companies.id
    and opening.date = '2011-01-18'
  inner join market closing
    on closing.company_id = companies.id
    and closing.date = '2011-02-16'
  where companies.ticker in ('goog','aapl','msft');

