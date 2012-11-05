#
# fluent-plugin-metricsense
#
# Copyright (C) 2012 Sadayuki Furuhashi
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluent::MetricSenseOutput::Backends

  class RDBTSDBBackend < Fluent::MetricSenseOutput::Backend
    include Fluent::Configurable

    Fluent::MetricSenseOutput::BACKENDS['rdb_tsdb'] = self

    config_param :rdb_url, :string
    config_param :rdb_table_prefix, :string
    config_param :rdb_read_url, :string, :default => nil

    INSERT_SUPPRESS_RING_BUFFER_SIZE = 64

    def initialize
      super
      require 'sequel'
      @insup_ring = []
      @insup_ring_index = 0
    end

    def configure(conf)
      super

      @rdb_read_url ||= @rdb_url

      @metric_tag_table = "#{@rdb_table_prefix}_metric_tags"
      @segment_value_table = "#{@rdb_table_prefix}_segment_values"
      @data_table = "#{@rdb_table_prefix}_data"
      @metric_view = "#{@rdb_table_prefix}_metrics"
      @metric_json_view = "#{@rdb_table_prefix}_json"

      #sql_standard_concat = lambda {|array| array.join(' || ') }
      #sql_standard_surround = lambda {|expr| "'\"' || #{expr} || '\"'" }

      case @rdb_url
      when /^mysql/i
        @sql_type = :mysql
        @sql_autoincr_type = "INT"
        @sql_autoincr_ref_type = "INT"
        @sql_autoincr_suffix = " AUTO_INCREMENT"
        @sql_value_type = "SMALLINT"
        @sql_name_type = "VARCHAR(255)"
        @sql_time_type = "INT"
        @sql_insert_ignore = "INSERT IGNORE"
        @sql_insert_returns_last_id = true
        #@sql_concat = lambda {|array| "CONCAT(#{array.join(', ')})" }
        #@sql_surround = lambda {|expr| "CONCAT('\"', #{expr}, '\"')" }
      when /^postgres/i
        @sql_type = :postgresql
        @sql_autoincr_type = "SERIAL"
        @sql_autoincr_ref_type = "INT"
        @sql_autoincr_suffix = ""
        @sql_value_type = "SMALLINT"
        @sql_name_type = "VARCHAR(255)"
        @sql_time_type = "INT"
        @sql_insert_ignore = "INSERT"
        @sql_insert_returns_last_id = false
        #@sql_concat = sql_standard_concat
        #@sql_surround = sql_standard_surround
      when /^sqlite/i
        @sql_type = :sqlite
        @sql_autoincr_type = "INTEGER"
        @sql_autoincr_ref_type = "INTEGER"
        @sql_autoincr_suffix = " AUTOINCREMENT"
        @sql_value_type = "INTEGER"
        @sql_name_type = "TEXT"
        @sql_time_type = "INTEGER"
        @sql_insert_ignore = "INSERT OR IGNORE"
        @sql_insert_returns_last_id = false
        #@sql_concat = sql_standard_concat
        #@sql_surround = sql_standard_surround
      else
        @sql_type = :unknown
        @sql_autoincr_type = "INT"
        @sql_autoincr_ref_type = "INT"
        @sql_autoincr_suffix = " AUTO_INCREMENT"
        @sql_value_type = "SMALLINT"
        @sql_name_type = "VARCHAR(255)"
        @sql_time_type = "INT"
        @sql_insert_ignore = "INSERT IGNORE"
        @sql_insert_returns_last_id = false
        #@sql_concat = sql_standard_concat
        #@sql_surround = sql_standard_surround
      end
    end

    def start
      ensure_connect do |db|
        db.run %[
          CREATE TABLE IF NOT EXISTS `#{@metric_tag_table}` (
            id #{@sql_autoincr_type} PRIMARY KEY#{@sql_autoincr_suffix},
            metric_name #{@sql_name_type} NOT NULL,
            segment_name #{@sql_name_type} NOT NULL,
            UNIQUE (metric_name, segment_name)
          );]

        db.run %[
          CREATE TABLE IF NOT EXISTS `#{@segment_value_table}` (
            id #{@sql_autoincr_type} PRIMARY KEY#{@sql_autoincr_suffix},
            name #{@sql_name_type} NOT NULL,
            UNIQUE (name)
          );]

        minutes = (0..59).to_a.map {|m| "m#{m} #{@sql_value_type} NOT NULL DEFAULT 0" }.join(', ')
        db.run %[
          CREATE TABLE IF NOT EXISTS `#{@data_table}` (
            base_time #{@sql_time_type} NOT NULL,
            metric_id #{@sql_autoincr_ref_type} NOT NULL,
            segment_id #{@sql_autoincr_ref_type},
            #{minutes},
            PRIMARY KEY (base_time, metric_id, segment_id)
          );]

        if @sql_type == :postgresql
          # ignore duplication error on data_table
          db.run %[
            CREATE OR REPLACE RULE ignore_duplicated_insert AS ON INSERT TO `#{@data_table}`
            WHERE NEW.base_time = OLD.base_time AND NEW.metric_id = OLD.metric_id AND NEW.segment_id = OLD.segment_id
            DO INSTEAD NOTHING;]
        end

        #minutes = (0..59).to_a.map {|m| "m#{m}" }.join(', ')
        #db.run %[
        #  CREATE VIEW IF NOT EXISTS `#{@metric_view}` AS
        #  SELECT
        #    base_time * 60 AS time,
        #    M.metric_name AS metric_name,
        #    CASE M.segment_name WHEN '' THEN NULL ELSE M.segment_name END AS segment_name,
        #    S.name AS segment_value,
        #    #{minutes}
        #  FROM `#{@data_table}` T
        #  LEFT JOIN `#{@metric_tag_table}` M ON T.metric_id = M.id
        #  LEFT JOIN `#{@segment_value_table}` S ON T.segment_id = S.id;]

        #minutes = (0..59).to_a.map {|m| ["m#{m}", "','"] }.flatten!
        #minutes.pop
        #minutes = @sql_concat.call(["'['"]+minutes+["']'"])
        #db.run %[
        #  CREATE VIEW IF NOT EXISTS `#{@metric_json_view}` AS
        #  SELECT
        #    base_time * 60 AS time,
        #    #{@sql_surround.call("M.metric_name")} AS metric_name,
        #    CASE WHEN M.segment_name IS NULL OR M.segment_name = '' THEN 'null' ELSE #{@sql_surround.call("M.segment_name")} END AS segment_name,
        #    CASE WHEN S.name IS NULL OR S.name = '' THEN 'null' ELSE #{@sql_surround.call("S.name")} END AS segment_value,
        #    #{minutes}
        #  FROM `#{@data_table}` T
        #  LEFT JOIN `#{@metric_tag_table}` M ON T.metric_id = M.id
        #  LEFT JOIN `#{@segment_value_table}` S ON T.segment_id = S.id;]

        reload_metric_names!(db)
        reload_segment_names!(db)
      end
    end

    #def shutdown
    #end

    ROW_TIME_WINDOW = 60*24
    ROW_TIME_WINDOW_BITS = 11
    ROW_TIME_WINDOW_MASK = (1<<ROW_TIME_WINDOW_BITS)-1

    def write(data)
      ensure_connect do |db|
        # group by row_key (base_time,metric_id,segment_id)
        rows = {}
        data.each {|(tag,time,value,seg_key,seg_val)|
          base_time = time / ROW_TIME_WINDOW
          metric_id = get_metric_id(db, tag, seg_key)
          segment_id = get_segment_id(db, seg_val) if seg_val

          row_key = [base_time, metric_id, segment_id]
          minutes = (rows[row_key] ||= [])
          minutes << ((value << ROW_TIME_WINDOW_BITS) | (time % 60))
        }

        # insert rows if not exist
        if @sql_type == :sqlite
          # sqlite3 < 1.3.7 doesn't allow multiple rows at once
          rows.keys.each {|row_key|
            next if @insup_ring.include?(row_key)
            db["#{@sql_insert_ignore} INTO `#{@data_table}` (base_time,metric_id,segment_id) VALUES (?,?,?)", *row_key].update
            rid = @insup_ring_index = (@insup_ring_index + 1) % INSERT_SUPPRESS_RING_BUFFER_SIZE
            @insup_ring[rid] = row_key
          }
        else
          insert_sql = "#{@sql_insert_ignore} INTO `#{@data_table}` (base_time,metric_id,segment_id) VALUES " + (["(?,?,?)"] * rows.size).join(', ')
          insert_params = [insert_sql]
          rows.keys.each {|row_key|
            next if @insup_ring.include?(row_key)
            insert_params.concat(row_key)
            rid = @insup_ring_index = (@insup_ring_index + 1) % INSERT_SUPPRESS_RING_BUFFER_SIZE
            @insup_ring[rid] = row_key
          }
          db[*insert_params].update
        end

        # increment values
        db.transaction do
          rows.each_pair {|row_key,minutes|
            update_sql = "UPDATE `#{@data_table}` SET "
            update_params = [update_sql]

            values_sql = []
            minutes.each {|m|
              value = m >> ROW_TIME_WINDOW_BITS
              minute = m & ROW_TIME_WINDOW_MASK
              values_sql << "m#{minute} = m#{minute} + ?"
              update_params << value
            }.join(', ')
            update_sql << values_sql.join(', ') << " WHERE base_time=? AND metric_id=? AND segment_id=?"
            update_params.concat(row_key)

            db[*update_params].update
          }
        end
      end
    end

    def reload_metric_names!(db)
      map = {}
      db.fetch("SELECT id, metric_name, segment_name FROM `#{@metric_tag_table}`") {|row|
        key = "#{row[:metric_name]}\0#{row[:segment_name]}"
        map[key] = row[:id]
      }
      @metric_names = map
    end

    def reload_segment_names!(db)
      map = {}
      db.fetch("SELECT id, name FROM `#{@segment_value_table}`") {|row|
        map[row[:name]] = row[:id]
      }
      @segment_names = map
    end

    def get_metric_id(db, tag, seg_key)
      key = "#{tag}\0#{seg_key}"
      id = @metric_names[key]
      return id if id

      begin
        id = db["INSERT INTO `#{@metric_tag_table}` (metric_name,segment_name) VALUES (?,?)", tag, seg_key||''].update
        if @sql_insert_returns_last_id
          @metric_names[key] = id
          return id
        end
        reload_metric_names!(db)
        return @metric_names[key]

      rescue => e
        reload_metric_names!(db)
        id = @metric_names[key]
        return id if id
        raise e
      end
    end

    def get_segment_id(db, seg_val)
      key = seg_val ? seg_val.to_s : ''
      id = @segment_names[key]
      return id if id

      begin
        id = db["INSERT INTO `#{@segment_value_table}` (name) VALUES (?)", key].update
        if @sql_insert_returns_last_id
          @segment_names[key] = id
          return id
        end
        reload_segment_names!(db)
        return @segment_names[key]

      rescue => e
        reload_segment_names!(db)
        id = @segment_names[key]
        return id if id
        raise e
      end
    end

    def ensure_connect(&block)
      db = Sequel.connect(@rdb_url, :max_connections=>1)
      begin
        block.call(db)
      ensure
        db.disconnect
      end
    end
  end

end
