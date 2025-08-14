# sqlalchemy
This is an example of the sqlalchemy-based ORMs I have written in my professional career. The core of the functionality for this ORM rests with the `PrimaryReader` object. This object is meant to capture as many table filtering and querying behaviors as possible while presenting those behaviors in more simplified calls for child-class readers. 

Each child-class reader can be expanded based on the needs of the developer and their use case. Expansion is made easy thanks to the `PrimaryReader` capturing and streamlining query construction, allowing devs to query columns using a sqlalchemy `BinaryExpression` variable and their chosen reader's associated Domain object, which represent the table of a SQL schema. 
