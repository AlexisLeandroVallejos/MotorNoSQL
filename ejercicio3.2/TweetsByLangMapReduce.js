    var mapFunction = function() {
        emit(this.lang, this.id);
    };

    var reduceFunction = function(keyLang, valuesIds) {
        return valuesIds.length
    };

    db.tweets.mapReduce(
        mapFunction,
        reduceFunction,
        {out:'tweets_by_lang_result'}
    );