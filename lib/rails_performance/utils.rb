module RailsPerformance
  class Utils
    def self.time
      Time.now.utc
    end

    def self.from_datetimei(datetimei)
      Time.at(datetimei, in: "+00:00")
    end

    # date key in redis store
    def self.cache_key(now = Date.today)
      "date-#{now}"
    end

    # write to current slot
    # time - date -minute
    def self.field_key(now = RailsPerformance::Utils.time)
      now.strftime("%H:%M")
    end

    # Fetch keys and values from Redis. For information on the :count argument,
    # see documentation for the Redis `scan` command:
    #   https://redis.io/docs/latest/commands/scan/#the-count-option
    #
    # @param [String] query Redis key or pattern to query
    # @param [Integer] scan_batch_size The "count" option for Redis `scan`
    #                                  command, default: 10
    # @param [<Integer] mget_batch_size Number of keys sent to Redis `mget`
    #                                   command at one time, default: 1000
    #
    # @return [Array, Array] [<Array of keys found, Array of key values]
    def self.fetch_from_redis(query, scan_batch_size: 10, mget_batch_size: 1000)
      RailsPerformance.log "\n\n   [REDIS QUERY]   -->   #{query}\n\n"

      keys = []
      current_cursor = nil
      until current_cursor.present? && current_cursor.zero?
        current_cursor ||= 0

        RailsPerformance.log "\n\n   [CURSOR]   -->   #{current_cursor}\n\n"

        current_cursor, key_batch = RailsPerformance.redis.scan(
          current_cursor,
          match: query,
          count: scan_batch_size,
          type: :string # only return String objects from Redis
        )
        keys += key_batch

        current_cursor = current_cursor.to_i
      end

      return [] if keys.blank?

      values = keys.in_groups_of(mget_batch_size).map do |key_batch|
        RailsPerformance.redis.mget(key_batch.compact)
      end.flatten

      RailsPerformance.log "\n\n   [FOUND]   -->   #{values.size}\n\n"

      [keys, values]
    end

    def self.save_to_redis(key, value, expire = RailsPerformance.duration.to_i)
      # TODO think here if add return
      # return if value.empty?

      RailsPerformance.log "  [SAVE]    key  --->  #{key}\n"
      RailsPerformance.log "  [SAVE]    value  --->  #{value.to_json}\n\n"
      RailsPerformance.redis.set(key, value.to_json, ex: expire.to_i)
    end

    def self.days(duration = RailsPerformance.duration)
      (duration / 1.day) + 1
    end

    def self.median(array)
      sorted = array.sort
      size = sorted.size
      center = size / 2

      if size == 0
        nil
      elsif size.even?
        (sorted[center - 1] + sorted[center]) / 2.0
      else
        sorted[center]
      end
    end

    def self.percentile(values, percentile)
      return nil if values.empty?

      sorted = values.sort
      rank = (percentile.to_f / 100) * (sorted.size - 1)

      lower = sorted[rank.floor]
      upper = sorted[rank.ceil]
      lower + (upper - lower) * (rank - rank.floor)
    end
  end
end
