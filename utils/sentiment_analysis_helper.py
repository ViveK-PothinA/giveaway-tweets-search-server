from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def analyze_line(line: str) -> dict:
    analyzer = SentimentIntensityAnalyzer()
    sentiment_dict = analyzer.polarity_scores(line)
    
    overall = "Neutral"
    if sentiment_dict['compound'] >= 0.05:
        overall = "Positive"
    elif sentiment_dict['compound'] <= - 0.05:
        overall = "Negative"
    else:
        overall = "Neutral"

    return {"positive": f"{sentiment_dict['pos']:.3f}", "negative": f"{sentiment_dict['neg']:.3f}", "neutral": f"{sentiment_dict['neu']:.3f}", "overall": overall}


def analyze_json_arr(arr: list[dict]) -> list[dict]:
    
    for d in arr:
        tweet = d['tweet']
        sentiment = analyze_line(tweet)

        d['sentiment'] = sentiment

    return arr
    