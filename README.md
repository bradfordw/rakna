Räkna - You Count On It!
=================

#### 1 Overview
#### 2 Quick Start 
    2.1 Building Räkna
    2.2 Starting Räkna
    2.3 REST API 
    2.4 Erlang API
    2.5 Querying

#### 1 Overview


Räkna, pronounced: "wreck nah" according to Google's English->Swedish translation, is intended to be a time-series based, (soon-to-be) distributed, incremental/decremental 
counting mechanism with support for integers and floats.

Why?

I wanted a way to reliably store counters over a period of time (dates for now). I need this for everything from counting allele frequencies coming off of genotyping instrumentation to fantasy-sports points gathering.
You could also throw in web-metrics for gits and shiggles, but I think that's one of the more obvious use cases.

What's under the hood? (aside from the rushed API)

 Basho's eleveldb storage for persistent data for starters.
 Ulf Wiger's sext library for aiding leveldb with sequential writes (took this trick from the leveldb driver in Riak).
 Basho's webmachine for a rock-solid REST API.
 Mochiweb via Webmachine for JSON encoding


#### 2 Quick Start


	2.1 Building Räkna

		Via rebar:
		1. ./rebar get-deps
		2. ./rebar compile
		3. ./rebar generate

	2.2 Starting Räkna

		./rel/rakna-node/bin/rakna-node [start|console] (pick either one)

	2.3 Räkna REST API

		Assuming you used the defaults, you can access counters this way:
		GET http://127.0.0.1:8088/counters/[counter name]
			Be sure to use a Content-Type of: application/json
			
	2.4 Erlang API

		Assuming you are running in a console, or have it loaded via an application:

		Incrementing

		rakna_node:increment(Key) - where Key is the binary label, today's date() will be used.
		rakna_node:increment(Key, Amount)
		rakna_node:increment({Date :: tuple(), Key})
		rakna_node:increment({Date :: tuple(), Key}, Amount)
		
		Decrementing

		rakna_node:decrement(Key) - where Key is the binary label, today's date() will be used
		rakna_node:decrement(Key, Amount)
		rakna_node:decrement({Date :: tuple(), Key})
		rakna_node:decrement({Date :: tuple(), Key}, Amount)
		
		Reading

		rakna_node:get_counter(Date :: tuple(), Key)
		rakna_node:get_counter(Date :: tuple(), Key, Aggregates :: list(min, max, last, delta))

		All increment/decrement functions are synchronous. For asynchronous versions use the a_increment/a_decrement version.

		You can configure labels to aggregate their minimum, maximum, last and a delta of the previous/current values.
		These are not yet reflected in the web api, but will be there shortly.
	2.5 Querying
		
		Querying is currently a work in progress, but functional and currently only available via Erlang but will become available over REST very soon.
		
		In the src/rakna_query.hrl file exists an rq_predicate record which consists of a few fields: rq_predicate, interval, label, aggregate and value.
		You can provide this record with any of these fields to the rakna_node:select/1 function.
		
		So for a few examples I'll provide one for each field of the rq_predicate record. Please note that when you add things to your "clause" they do so as an "and" - "or" is not supported.
		
		interval - currently just the date, may (in time) add deeper precision (hours, minutes...)
		rakna_node:select(#rq_predicate{interval=date()}) - match any label with a matching interval.
		rakna_node:select(#rq_predicate{interval={between, date1(), date2()}) - match any between these dates.
		rakna_node:select(#rq_predicate{interval=[date1(), date2(), date3()]}) - match that date is a member of this list of dates.
		rakna_node:select(#rq_predicate{interval={not_in, [date1(), date2(), date3()]}}) - match that date is NOT a member of this list.
		rakna_node:select(#rq_predicate{interval={greater_than, date()}}) - match dates greater than this.
		rakna_node:select(#rq_predicate{interval={less_than, date()}}) - match dates less than this.
		
		label - this is what you've supplied as some interesting "thing" you wish to count, for example a stock price or a team name.
		rakna_node:select(#rq_predicate{label=<<"foo">>}) - would match any interval of <<"foo">> (all-time). Add in interval=date() for more precision.
		rakna_node:select(#rq_predicate{label=[<<"foo">>,<<"bar">>]}) - would match labels which are a member of the list [<<"foo">>,<<"bar">>].
		rakna_node:select(#rq_predicate{label={not_in, [<<"foo">>,<<"bar">>]}}) - would match labels which are NOT a member of the list [<<"foo">>,<<"bar">>].
		* I do have plans to add regexp support in the near future. 

		aggregate - this is one of the pre-computed aggregate values for a label on an interval (min, max, last, delta).
		rakna_node:select(#rq_predicate{aggregate=[min,max]}) - give me all-time every minimum and maximum for every label I have. Again, trim it down by providing a label and/or interval.
		rakna_node:select(#rq_predicate{aggregate=all}) - give me all-time every aggregate for everything we have.
		rakna_node:select(#rq_predicate{aggregate=delta}) - give me all-time every aggregate-type (in this case, it's delta) for everything we have.
		
		value - this is the actual value of the counter. 
		rakna_node:select(#rq_predicate{value=140.54}) - give me all-time every value that's equal to this.
		rakna_node:select(#rq_predicate{value={between, 140.0, 150.0}}) - all labels whose value is between 140.0 and 150.0.
		rakna_node:select(#rq_predicate{value={greater_than, 140.54}}) - all greater than...
		rakna_node:select(#rq_predicate{value={less_than, 140.54}}) - all less than...
		rakna_node:select(#rq_predicate{value={greater_than_eq, 140.54}}) - all greater than or equal to...
		rakna_node:select(#rq_predicate{value={less_than_eq, 140.54}}) - all less than or equal to...
		rakna_node:select(#rq_predicate{value=[1.0,3.0,5.0]}) - all values which are a member of this list.
		
		
		A quick example of combining these. Let's say we want to see the label <<"det">>'s over the first week of February of last year.
		
		rakna_node:select(#rq_predicate{interval={between,{2011,2,1},{2011,2,15}}, label = <<"det">>}).
		
		Or we'd like to see just <<"det">>'s minimum and maximum for each interval in this range:
		
		rakna_node:select(#rq_predicate{interval={between,{2011,2,1},{2011,2,15}}, label = <<"det">>, aggregate=[min,max]}).
		
		