Räkna - You Count On It!
=================
1 Overview
2 Quick Start 
    2.1 Building Räkna
    2.2 Starting Räkna
    2.3 Räkna's REST API 


1 Overview
~~~~~~~~~~~
Räkna, pronounced: "wreck nah" according to Google's English->Swedish translation, is intended to be a time-series based, (soon-to-be) distributed, incremental/decremental 
counting mechanism with support for integers and floats.

	Why?
	I wanted a way to reliably store counters over a period of time (dates for now). I need this for everything from counting allele frequencies coming off of genotyping instrumentation to fantasy-sports points gathering.
	You could also throw in web-metrics for gits and shiggles, but I think that's one of the more obvious use cases.

	What's under the hood? (aside from the rushed API)
		Basho's eleveldb storage for persistent data for starters.
		Ulf Wiger's sext library for aiding leveldb with sequential writes (took this trick from the leveldb driver in Riak).
		Basho's webmachine for a rock-solid REST API.
		In-directly mochiweb via webmachine for JSON encoding (Hi Bob!)
		And I'm sure there is something from Mr. Virding in here as well...
		See a theme here?

2 Quick Start
~~~~~~~~~~~
	2.1 Building Räkna
	2.2 Starting Räkna
	2.3 Räkna REST API
