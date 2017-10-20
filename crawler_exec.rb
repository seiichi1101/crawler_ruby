# -*- coding: utf-8 -*-
require "twitter"
require "mysql"
require "cassandra"
require "date"
require "inifile"

class Fetching_tw
    def initialize(q)
        begin
            inifile = IniFile.load("./keys.ini")
            @data=[]
            @since_id = 0
            @query = q
            @client = Twitter::REST::Client.new do |config|
                config.consumer_key        = _get(inifile, "twitter", 'config.consumer_key')
                config.consumer_secret     = _get(inifile, "twitter", 'config.consumer_secret')
                config.access_token        = _get(inifile, "twitter", 'config.access_token')
                config.access_token_secret = _get(inifile, "twitter", 'config.access_token_secret')
            end

        rescue Twitter::Error::ClientError
            puts "**Error**"
            sleep(60)
            retry
        end
    end

    def fetching
        @data = []
        begin
            result_tweets = @client.search(@query, result_type:"recent", exclude: "retweets", since_id:@since_id)
            result_tweets.take(50).each_with_index do |tw, i|
                @data[i] = [tw.id, tw.user.screen_name, tw.full_text]
                @since_id = tw.id
            end
        rescue Twitter::Error::ClientError
            puts "**Error**"
            sleep(60)
            retry
        end
      #  p @data
    end

    def getData()
        return @data
    end

end

def _get(inifile, section, name)
  begin
    return inifile[section][name]
  rescue => e
    return "error: could not read #{name} msg:"+e
  end
end

query = ""

ARGV.each do |arg|
    query += arg.to_s+" "
end

if (query == "") then
  puts ("No arguments are provided")
  exit(1)
end

cluster = Cassandra.cluster

keyspace_name = 'twitter'
session  = cluster.connect(keyspace_name)

table_definition = <<-TABLE_CQL
  CREATE TABLE IF NOT EXISTS #{query} (
    id VARCHAR,
    name VARCHAR,
    text TEXT,
    date TIMESTAMP,
    PRIMARY KEY (id)
  )
TABLE_CQL

session.execute(table_definition)

statement = session.prepare('INSERT INTO weed (id, name, text, date) VALUES (?, ?, ?, ?)')

puts ("クエリ： "+query)

tw = Fetching_tw.new(query)
while true
    puts ("---------------------get data from twitter-----------------------")
    tw.fetching
    puts ("---------------------put data to cassandra-----------------------")
    tw.getData.each do |elt|
      elt[2] = elt[2].gsub(/['']/) {|ch| ch + ch }
      session.execute(statement, arguments: [elt[0].to_s, elt[1].to_s, elt[2].to_s, DateTime.now.to_time])
    end
    puts ("---------------------sleep for 60s-----------------------")
    sleep(60)
end
