require "colors"
elasticsearch = require "elasticsearch"
debug = require("debug")("WATERLINE-SAILS-ES")
debug.error = require("debug")("WATERLINE-SAILS-ES-ERROR")
async = require "async"
_ = require "lodash"
uuid = require "node-uuid"
module.exports = {
	find : (connections, conn, coll, options, cb)->
		client = connections[conn]
		query = new Object()
		query.index = client.options.index
		query.type = coll
		query.body = {}
		f_query = undefined

		if options.limit
			query.body = {}
			query.body.size = options.limit

		if options.skip
			query.body.from = options.skip

		if options.sort
			query.sort = []
			for k, v of options.sort
				t = "asc"
				if v is -1
					t = "desc"
				query.sort.push "#{k}:#{t}"

		if options.where and options.where.id
			if typeof options.where.id is "object"
				query.body.ids = options.where.id
				f_query = "mget"
			else
				query.id = options.where.id
				delete query.body
				f_query = "get"
		else
			unless options.limit
				query.body.size = 200
				
			unless options.skip
				query.body.from = 0
			# query.q = "_class:#{options.where._class}"
			
			query.bool = {
				must: []
				must_not: []
				should: []
			}

			f_query = "search"
			### BUILD QUERY  ####

		# debug "QUERY", f_query, query

		##### QUERY #####
		client[f_query] query
		.then (results)->
			# debug f_query, query, results
			if results._source
				return cb null, [results._source]

			if results.docs
				docs = _.map results.docs, (d)->
					d._source
				return cb null,	docs

			if results.hits
				docs = _.map results.hits.hits, (d)->
					d._source
				return cb null, docs

		.catch (errors)->
			cb errors.toString()
}