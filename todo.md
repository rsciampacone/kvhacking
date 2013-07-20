TODO
====

Things that get thought of that may or may not be relevant / bogus.

* Key fetching and type checking is a common operation.  Group with reply?
But how to get the reply as part of the general fetch operation.  Fear of having
the reply becoming a part of the fabric of the datastore commands.  Maybe the reply_
mechanism is using an object that handles and does the appropriate thing?
(ie: @connection)
* Have @connection be the control point for replies.  This allows different mechanisms
to be plugged in?
* switch the cmd_ type operations to be suffixed with their arg counts e.g., _3 or _2+.
Have the lookup mechanism for DNU then figure out what the appropriate count is based on
an existing method and dispatch through that.  Barring a proper method count, single
place to handle wrong arg count.  Perhaps a bit heavy handed?
