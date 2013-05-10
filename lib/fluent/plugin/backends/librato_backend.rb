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

  require 'net/http'
  require 'cgi'
  require 'json'

  class LibratoBackend < Fluent::MetricSenseOutput::Backend
    Fluent::MetricSenseOutput.register_backend('librato', self)

    config_param :librato_user, :string
    config_param :librato_token, :string

    def initialize
      super
      @initialized_metrics = {}
    end

    def write(data)
      http = Net::HTTP.new('metrics-api.librato.com', 443)
      http.open_timeout = 60
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE  # FIXME verify
      #http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      http.cert_store = OpenSSL::X509::Store.new
      header = {}

      begin
        # send upto 50 entries at once
        data.each_slice(50) {|slice|
          req = Net::HTTP::Post.new('/v1/metrics', header)
          req.basic_auth @librato_user, @librato_token

          data = []
          slice.each_with_index {|(tag,time,value,seg_key,seg_val,mode),i|
            if seg_key
              name = "#{tag}:#{seg_key}"
              source = seg_val
            else
              name = tag
              source = nil
            end
            h = {
              "name" => name,
              "measure_time" => time,
              "value" => value,
            }
            h["source"] = source.to_s if source
            data << h
            ensure_metric_initialized(http, name, mode)
          }
          body = {"gauges"=>data}.to_json

          $log.trace { "librato metrics: #{data.inspect}" }
          req.body = body
          req.set_content_type("application/json")
          res = http.request(req)

          # TODO error handling
          if res.code != "200"
            $log.warn "librato_metrics: #{res.code}: #{res.body}"
          end
        }

      ensure
        http.finish if http.started?
      end
    end

    METRIC_INITIALIZE_REQUEST_PER_MODE = []

    METRIC_INITIALIZE_REQUEST_PER_MODE[UpdateMode::ADD] = {
        "type" => "gauge",
        "attributes" => {
          "source_aggregate" => true,
          "summarize_function" => "sum",
        }
      }.to_json

    METRIC_INITIALIZE_REQUEST_PER_MODE[UpdateMode::MAX] = {
        "type" => "gauge",
        "attributes" => {
          "summarize_function" => "max",
        }
      }.to_json

    def ensure_metric_initialized(http, name, mode)
      return if @initialized_metrics[name]

      header = {}
      req = Net::HTTP::Put.new("/v1/metrics/#{CGI.escape name}", header)
      req.basic_auth @librato_user, @librato_token

      $log.trace { "librato initialize metric with mode #{mode}: #{name}" }
      req.body = METRIC_INITIALIZE_REQUEST_PER_MODE[mode]
      req.set_content_type("application/json")
      res = http.request(req)

      # TODO error handling
      if res.code !~ /20./
        $log.warn "librato_metrics: #{res.code}: #{res.body}"
      else
        @initialized_metrics[name] = true
      end
    end
  end

end

