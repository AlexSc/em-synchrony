begin
  require "em-mongo"
rescue LoadError => error
  raise "Missing EM-Synchrony dependency: gem install em-mongo"
end

module EM
  module Mongo
    class Database
      def authenticate(username, password)
        auth_result = self.collection(SYSTEM_COMMAND_COLLECTION).first({'getnonce' => 1})

        auth                 = BSON::OrderedHash.new
        auth['authenticate'] = 1
        auth['user']         = username
        auth['nonce']        = auth_result['nonce']
        auth['key']          = EM::Mongo::Support.auth_key(username, password, auth_result['nonce'])

        auth_result2 = self.collection(SYSTEM_COMMAND_COLLECTION).first(auth)
        if EM::Mongo::Support.ok?(auth_result2)
          true
        else
          raise AuthenticationError, auth_result2["errmsg"]
        end
      end

      alias :acollections :collections
      def collections
        Synchrony.sync acollections
      end
    end

    class Connection
      def initialize(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil, opts = {})
        f = Fiber.current

        @em_connection = EMConnection.connect(host, port, timeout, opts)
        @db = {}

        # establish connection before returning
        EM.next_tick { f.resume }
        Fiber.yield
      end
    end

    class Collection
      def instrument(name, payload = {}, &blk)
        res = yield
        log_operation(name, payload)
        res
      end

      def log_operation(name, payload)
        @logger ||= nil
        return unless @logger
        msg = "#{payload[:database]}['#{payload[:collection]}'].#{name}("
        msg += payload.values_at(:selector, :document, :documents, :fields ).compact.map(&:inspect).join(', ') + ")"
        msg += ".skip(#{payload[:skip]})"  if payload[:skip]
        msg += ".limit(#{payload[:limit]})"  if payload[:limit]
        msg += ".sort(#{payload[:order]})"  if payload[:order]
        @logger.debug "MONGODB #{msg}"
      end

      #
      # The upcoming versions of EM-Mongo change Collection#find's interface: it
      # now returns a deferrable cursor YAY. This breaks compatibility with past
      # versions BOO. We'll just choose based on the presence/absence of
      # EM::Mongo::Cursor YAY
      #

      #
      # em-mongo version > 0.3.6
      #
      if defined?(EM::Mongo::Cursor)

        # afind     is the old (async) find
        # afind_one is rewritten to call afind
        # find      is sync, using a callback on the cursor
        # find_one  is sync, by calling find and taking the first element.
        # first     is sync, an alias for find_one

        alias :afind :find
        def find(*args)
          cursor = afind(*args)
          instrument(:find, :database => @db, :collection => name, :selector => cursor.selector, :fields => cursor.fields, :skip => cursor.skip, :limit => cursor.limit, :order => cursor.order) do
            Synchrony.sync cursor.to_a
          end
        end

        # need to rewrite afind_one manually, as it calls 'find' (reasonably
        # expecting it to be what is now known as 'afind')

        def afind_one(spec_or_object_id=nil, opts={})
          spec = case spec_or_object_id
                 when nil
                   {}
                 when BSON::ObjectId
                   {:_id => spec_or_object_id}
                 when Hash
                   spec_or_object_id
                 else
                   raise TypeError, "spec_or_object_id must be an instance of ObjectId or Hash, or nil"
                 end
          afind(spec, opts.merge(:limit => -1)).next_document
        end
        alias :afirst :afind_one

        def find_one(selector={}, opts={})
          log = { :database => @db, :collection => name, :selector => selector }
          log[:fields] = opts[:fields] if opts[:fields]
          log[:skip] = opts[:skip] if opts[:skip]
          log[:limit] = opts[:limit] if opts[:limit]
          log[:order] = opts[:order] if opts[:order]
          instrument(:find, log) do
            Synchrony.sync afind_one(selector, opts)
          end
        end
        alias :first :find_one

        alias :asafe_insert :safe_insert
        def safe_insert(*args)
          instrument(:insert, :database => @db, :collection => name, :documents => args[0]) do
            Synchrony.sync asafe_insert(*args)
          end
        end

        alias :asafe_update :safe_update
        def safe_update(*args)
          instrument(:update, :database => @db, :collection => name, :selector => args[0]) do
            Synchrony.sync asafe_update(*args)
          end
        end

        alias :asafe_remove :safe_remove
        def safe_remove(*args)
          instrument(:remove, :database => @db, :collection => name, :selector =>args[0]) do
            Synchrony.sync asafe_remove(*args)
          end
        end

      #
      # em-mongo version <= 0.3.6
      #
      else

        alias :afind :find
        def find(selector={}, opts={})

          f = Fiber.current
          cb = proc { |res| f.resume(res) }

          skip  = opts.delete(:skip) || 0
          limit = opts.delete(:limit) || 0
          order = opts.delete(:order)

          @connection.find(@name, skip, limit, order, selector, nil, &cb)
          Fiber.yield
        end

        # need to rewrite afirst manually, as it calls 'find' (reasonably
        # expecting it to be what is now known as 'afind')

        def afirst(selector={}, opts={}, &blk)
          opts[:limit] = 1
          afind(selector, opts) do |res|
            yield res.first
          end
        end

        def first(selector={}, opts={})
          opts[:limit] = 1
          find(selector, opts).first
        end
      end

    end

  end
end
