import requests
import json
url = "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/minutely"

querystring = {"lat":"35.5","lon":"-78.5"}

headers = {
	"X-RapidAPI-Key": "c12a4fcf8emshf19712541926cdcp19ec27jsn683bd634421e",
	"X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
data=json.loads(response.text)

json.dump(data, open('data1.json', 'w'))