small object mapping library on top of the hector cassandra client

NOTE: Due to a bug how cglib handles field annotations, the library
has been repackaged with a feedly prefix and included. The cglib
version used is 2.2.2. Once the bug is fixed, everything under the 
feedly.* package can be removed. cglib also seems to strip generic type 
information.

http://sourceforge.net/tracker/?func=detail&aid=2796998&group_id=56933&atid=482368

DONE:
+ apply schema (basic family creation)
+ find by key (single/bulk)
+ load by key (single/bulk)
+ handle values with list/maps
+ handle unmapped values
+ load partial (excludes don't work great, can't do exclusive range searches...)
+ allow specifying serializers for columns and unmapped, also collection key/values
+ add CF creation options - compressed.
+ create indices in schema

TODO:
+ find by native index
+ custom indexes (find/save)
+ TTL support?
+ counter support?

POSTPONE:
+ lazy? -- postpone - won't work great until exclusive ranges are supported
+ load partial efficiently - support excludes better if range search supports exclusive ranges
