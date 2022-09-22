select * 
from {{ metrics.calculate(
    metric('retweets'),
    grain='day',
    dimensions=['language_name','source_name']
) }}
