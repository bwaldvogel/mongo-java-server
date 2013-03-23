
t = db.regex4;
t.drop();

t.save( { name : "eliot" } );
t.save( { name : "emily" } );
t.save( { name : "bob" } );
t.save( { name : "aaron" } );

assert.eq( 2 , t.find( { name : /^e.*/ } ).count() , "no index count" );

t.ensureIndex( { name : 1 } );

assert.eq( 2 , t.find( { name : /^e.*/ } ).count() , "index count" );
