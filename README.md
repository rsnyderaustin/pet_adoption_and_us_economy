### Development Notes
Using AWS Parameter Store for AWS and API keys
    Add Parameter Store and Secrets Manager Lambda Extension after Lambda program is completed
    https://docs.aws.amazon.com/systems-manager/latest/userguide/ps-integration-lambda-extensions.html
Charts to display:
Number of pets published to adoption compared to each of 5 different tags

### Petfinder API
Individual API requests are stored in a separate class file within the AWS Lambda Python package.


### FRED API
Unlike the Petfinder API, the current simplicity of our API calls to the FRED API don't require the storage of 
any API request configs. API requests are simply made by sequentially requesting data for each valid series ID.
In the FRED API, our data is requested via 'series id'. The valid series ID's and their corresponding
data series are listed below:
1. GDP: Gross Domestic Product
2. RSXFS: Advance Retail Sales
3. UNRATE: Unemployment Rate
4. CPALTT01USM657N: Consumer Price Index: All Items
5. DFF: Federal Funds Effective Rate

# Website
### Charting Data

Current plan is to use ChartJS to display data on the website:
https://www.chartjs.org/docs/latest/getting-started/

Planned charts:
1. Number of dogs published to adoption compared to each FRED series
2. Number of cats published to adoption compared to each FRED series
3. Number of dogs published to adoption by size compared to each FRED series
4. Number of cats published to adoption by size compared to each FRED series

Required data for planned charts:
1. Number of dogs published to adoption per time interval
2. Number of cats published to adoption per time interval
3. Updated FRED economic series per time interval (FRED series are updated at different intervals)
4. Number of dogs published to adoption by size per time interval
5. Number of cats published to adoption by size per time interval

# Future Plans
Display pets adoption stats by state compared to state FRED time series via heat map.
