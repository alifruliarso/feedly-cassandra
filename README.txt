small object mapping library on top of the hector cassandra client

NOTE: Due to a bug how cglib handles field annotations, the library
has been repackaged with a feedly prefix and included. The cglib
version used is 2.2.2. Once the bug is fixed, everything under the 
feedly.* package can be removed. cglib also seems to strip generic type 
information.

http://sourceforge.net/tracker/?func=detail&aid=2796998&group_id=56933&atid=482368