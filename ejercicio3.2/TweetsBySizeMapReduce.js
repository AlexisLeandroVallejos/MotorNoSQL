var mapFunction = function() {
    if (this.text.length < 10) {
        emit("short", this.id);
    } else if (this.text.length < 20) {
        emit("medium", this.id);
    } else {
        emit("large", this.id);
    }
 };

 var reduceFunction = function(keyTextSize, valuesIds) {
    return valuesIds.length
 };

 db.tweets.mapReduce(
    mapFunction,
    reduceFunction,
    {out:'tweets_by_size_result'}
 );