
t = db.update_setOnInsert;

function dotest( useIndex ) {
    t.drop();
    if ( useIndex ) {
        t.ensureIndex( { a : 1 } );
    }

    t.update( { _id: 5 }, { $inc : { x: 2 }, $setOnInsert : { a : 3 } }, true );
    assert.eq( { _id : 5, x : 2, a: 3 }, t.findOne() );

    t.update( { _id: 5 }, { $set : { a : 4 } }, true );

    t.update( { _id: 5 }, { $inc : { x: 2 }, $setOnInsert : { a : 3 } }, true );
    assert.eq( { _id : 5, x : 4, a: 4 }, t.findOne() );
}

dotest( false );
dotest( true );
