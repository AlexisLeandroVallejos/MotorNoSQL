var mapFunction = function() {
    emit(this.source, this.user.id);
 };

 var reduceFunction = function(keySource, valuesUserIds) {
     //return valuesUserIds.filter(function (value, index, self) {return self.indexOf(value) === index;}).length;
    return valuesUserIds.length
 };

 db.tweets.mapReduce(
    mapFunction,
    reduceFunction,
    {out:'users_by_source_result'}
 );
