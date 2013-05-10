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

  class StdoutBackend < Fluent::MetricSenseOutput::Backend
    Fluent::MetricSenseOutput.register_backend('stdout', self)

    def write(data)
      data.each {|tag,time,value,seg_key,seg_val,mode|
        if seg_key
          puts "#{Time.at(time)} #{tag}: #{value}"
        else
          puts "#{Time.at(time)} #{tag} [#{seg_key}=#{seg_val}]: #{value}"
        end
      }
    end
  end

end

