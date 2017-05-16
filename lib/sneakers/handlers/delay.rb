module Sneakers
  module Handlers
    #
    # Delay setups a different set of queues and exchange to achieve delay with the dead letter policies.
    # Uses cascading time delay loops until the sheduled time.
    #
    # enqueue with following parameters:
    # - to_queue - queue name for the worker with this handler
    #              or enqueue directly (queue_as) 
    # - headers
    #   - work_at - time of execution
    #   - work_queue - destination queue for actually doing the work
    #
    class Delay
      def initialize(channel, queue, opts)
        @worker_queue_name = queue.name
        Sneakers.logger.debug do
          "#{log_prefix} creating handler, opts=#{opts}"
        end

        @channel = channel
        @opts = opts

        # Construct names, defaulting where suitable
        delay_name = @opts[:delay_exchange] || "#{@worker_queue_name}-delay"
        requeue_name = @opts[:delay_requeue_exchange] || "#{@worker_queue_name}-delay-requeue"
        # default delays are 1min, 10min, 1hr, 24hr
        @delay_periods = @opts[:delay_periods] || [60, 600, 3600, 86400]
        # make sure it is in descending order
        @delay_periods.sort!{ |x,y| y <=> x }

        # Create the exchanges
        Sneakers.logger.debug { "#{log_prefix} creating exchange=#{delay_name}" }
        @delay_exchange = @channel.exchange(delay_name, :type => 'headers', :durable => exchange_durable?)
        Sneakers.logger.debug { "#{log_prefix} creating exchange=#{requeue_name}" }
        @requeue_exchange = @channel.exchange(requeue_name, :type => 'topic', :durable => exchange_durable?)

        @delay_periods.each do |t|
          # Create the queues and bindings
          Sneakers.logger.debug do
            "#{log_prefix} creating queue=#{delay_name}-#{t} x-dead-letter-exchange=#{requeue_name}"
          end
          delay_queue = @channel.queue("#{delay_name}-#{t}",
                                       :durable => queue_durable?,
                                       :arguments => {
                                         :'x-dead-letter-exchange' => requeue_name,
                                         :'x-message-ttl' => t * 1000
                                       })
          delay_queue.bind(@delay_exchange, :arguments => { :delay => t })
        end
        queue.bind(@requeue_exchange, :routing_key => @worker_queue_name)

        @main_exchange = @channel.exchange(@opts[:exchange], @opts[:exchange_options])
      end

      def acknowledge(hdr, props, msg)
        @channel.acknowledge(hdr.delivery_tag, false)
      end

      def reject(hdr, props, msg, requeue = false)
        if requeue
          # This was explicitly rejected specifying it be requeued so we do not
          # want it to pass through our delay logic.
          @channel.reject(hdr.delivery_tag, requeue)
        else
          handle_delay(hdr, props, msg, :reject)
        end
      end

      def error(hdr, props, msg, err)
        reject(hdr, props, msg)
      end

      def timeout(hdr, props, msg)
        reject(hdr, props, msg)
      end

      def noop(hdr, props, msg)

      end

      def handle_delay(hdr, props, msg, reason)
        delay_period = next_delay_period(props[:headers])
        if delay_period > 0
          # We will publish the message to the delay exchange              
          Sneakers.logger.info do
            "#{log_prefix} msg=delaying, delay=#{delay_period}, headers=#{props[:headers]}"
          end
          @delay_exchange.publish(msg, :routing_key => hdr.routing_key, :headers => props[:headers].merge({ 'delay' => delay_period }))
          @channel.acknowledge(hdr.delivery_tag, false)
          # TODO: metrics
        else
          # Publish the original message with the routing_key to the queue exchange
          work_queue = props[:headers]['work_queue']
          Sneakers.logger.info do
            "#{log_prefix} msg=publishing, queue=#{work_queue}, headers=#{props[:headers]}"
          end

          @main_exchange.publish(msg, :routing_key => work_queue)
          @channel.acknowledge(hdr.delivery_tag, false)
          # TODO: metrics
        end
      end
      private :handle_delay

      def next_delay_period(headers)
        work_at = headers['work_at']
        t = (work_at - Time.current.to_f).round
        # greater check is to ignore remainder of time (seconds) smaller than the last delay
        @delay_periods.bsearch{ |e| t >= e && (t / e.to_f).round > 0 } || 0
      end
      private :next_delay_period

      # Prefix all of our log messages so they are easier to find. We don't have
      # the worker, so the next best thing is the queue name.
      def log_prefix
        "Delay handler [queue=#{@worker_queue_name}]"
      end
      private :log_prefix

      private

      def queue_durable?
        @opts.fetch(:queue_options, {}).fetch(:durable, false)
      end

      def exchange_durable?
        queue_durable?
      end

    end
  end
end
