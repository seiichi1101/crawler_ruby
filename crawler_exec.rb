# -*- coding: utf-8 -*-
require "twitter"
require "mysql"
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
                #  次はこのID以降のTweetが取得される
                @since_id = tw.id
            end
                # 検索ワードで Tweet を取得できなかった場合の例外処理
        rescue Twitter::Error::ClientError
            puts "**Error**"
            sleep(60)
            retry
        end
#        p @data
    end

    def getData()
        return @data
    end

end
class Mymysql
    def initialize(q)
        #    tblname="tbl_"+DateTime.now.strftime("%Y_%m_%d_%H_%M_%S").to_s
        @dbname="tw_data"
        @tblname=q

        @client= Mysql.connect("127.0.0.1", "root", "root")
        @client.query("set character set utf8mb4")
        @client.query("CREATE DATABASE IF NOT EXISTS #{@dbname}")
        @client.query("USE #{@dbname}")
        @client.query("create table if not EXISTS #{@tblname} (id char(18), name char(32), text varchar(255), date DATETIME(6))")
    end
    def set_to_mysql(data)
      date = DateTime.now.strftime('%Y-%m-%d %I:%M:%S')
      puts data
      data.each do |elt|
        elt[2] = elt[2].gsub(/['']/) {|ch| ch + ch }
        @client.query("insert into #{@tblname}(id, name, text, date)VALUES('#{elt[0].to_s}', '#{elt[1].to_s}', '#{elt[2].to_s}', '#{date}')")
      end
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


puts ("クエリ： "+query)

tw = Fetching_tw.new(query)
while true
    puts ("---------------------get data from twitter-----------------------")
    tw.fetching
    puts ("---------------------put data to mysql-----------------------")
    sql = Mymysql.new(query)
    sql.set_to_mysql(tw.getData())
    puts ("---------------------sleep for 60s-----------------------")
    sleep(60)
end
