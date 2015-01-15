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

  class DatadogBackend < Fluent::MetricSenseOutput::Backend
    Fluent::MetricSenseOutput.register_backend('datadog', self)

    config_param :dd_api_key, :string
    config_param :dd_app_key, :string, :default => nil
    config_param :host, :string, :default => nil
    config_param :tags, :array, :default => []
    config_param :batch_size, :integer, :default => 200

    def initialize()
      super
      require "dogapi"
    end

    def configure(conf)
      super

      if @dd_api_key.nil?
        raise Fluent::ConfigError, "missing Datadog API key"
      end

      client_args = [@dd_api_key]
      client_args << @dd_app_key if @dd_app_key
      @dog = Dogapi::Client.new(*client_args)
    end

    def write(data)
      data.each_slice(@batch_size) do |slice|
        metric_points = {}
        slice.each do |tag, time, value, seg_key, seg_val|
          if seg_key and seg_val
            # segmented values
            segment = "#{seg_key}:#{seg_val}"
          else
            # simple values
            segment = "simple"
          end
          metric = tag # use fluentd's tag as metric name on datadog
          metric_points[metric] ||= {}
          metric_points[metric][segment] ||= []
          metric_points[metric][segment].push([Time.at(time), value])
        end

        metric_points.each do |metric, segment_points|
          segment_points.each do |segment, points|
            seg_key, seg_val = segment.split(":", 2)

            tags = @tags.dup
            tags.push(segment)
            if seg_key and seg_val
              # add seg_key as a tag to allow calculating metrics over the segment name
              tags.push(seg_key)
            end

            options = {}
            options[:tags] = tags
            options[:host] = @host if @host
            options[:type] = "gauge"

            log.debug("datadog emit points: metric=#{metric}, points=#{points.inspect}, options=#{options.inspect}")
            code, response = @dog.emit_points(metric, points, options)
            if code.to_i / 100 != 2
              raise("datadog returns #{code}: #{response.inspect}")
            end
          end
        end
      end
    end
  end
end
