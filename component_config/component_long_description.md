# Google My Business Extractor

### API Document

[Google My Business API](https://developers.google.com/my-business/reference/rest/v4/)

### Account Authorization

To avoid automatically login to any Gmail accounts available in your browser, please authorize the extractor via 'Incognito' mode/ 'private' mode.

If you are using your personal Google Account to manage locations for your business or company, please ensure your account has the access right to the locations you are interested in. For a list of Google My Business account types, please visit [account types](https://developers.google.com/my-business/content/accounts).

### Configuration

1. Endpoints

    1. Location Insights
    2. Reviews
    3. Media
    4. Questions

2. Request Range
    - This configuration will only affect [Location Insights] endpoint. Google My Business API has limited the maximum range of request to 18 months. By default, the component is configured to fech data from 1 week ago to the date of the component run