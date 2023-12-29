### Development Notes
DynamoDB Pricing: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html#ItemSizeCalculations.Reads

Packing Python for CI/CD: https://towardsdatascience.com/create-your-custom-python-package-that-you-can-pip-install-from-your-git-repository-f90465867893

Read and write units are rounded up to 4 KB increments per item. Thus, storing individual numbers for each day of the 
month like I was previously planning is extremely inefficient. Instead, items should be stored by month, with an 
attribute storing the month's data as a JSON formatted {date:value}.

Using AWS Parameter Store for AWS and API keys
    Add Parameter Store and Secrets Manager Lambda Extension after Lambda program is completed
    https://docs.aws.amazon.com/systems-manager/latest/userguide/ps-integration-lambda-extensions.html

Charts to display:
Number of pets published to adoption compared to each of 5 different tags

Program runs at 2 AM every day.

### Petfinder API



### FRED API
Data is updated from the day after the last update through the 'observation_start' parameter in order to account for any 
possible outages on previous days. API requests are simply made by sequentially requesting data for each valid series ID.
In the FRED API, our data is requested via 'series id'. The valid series ID's and their corresponding
data series are listed below:
1. GDP: Gross Domestic Product - Updated Quarterly
2. RSXFS: Advance Retail Sales - Updated Monthly
3. UNRATE: Unemployment Rate - Updated Monthly
4. CPALTT01USM657N: Consumer Price Index: All Items - Updated Monthly
5. DFF: Federal Funds Effective Rate - Updated Daily

# AWS
### Choosing a Database
As the project currently stands, data will only be read and written into the database daily. Thus, high throughput
isn't necessary for a database. An important decision is using a NoSQL or a RDBMS. The primary factors in this decision 
are cost and conformity to our access patterns. DynamoDB is the best choice as the database will only have to be read and 
written on once per day, which should keep the
project within the free tier. Additionally, the access patterns are consistently based on only two keys: by series ID's 
and dates for FRED, and by animal type and dates for Petfinder. The consistent access patterns around just two keys is 
ideal for DynamoDB. So, DynamoDB is the database of choice for this project.

### Database Format
-- Need to insert link to database example image --

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
