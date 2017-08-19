import pandas as pd
import requests

# Import current scrub.csv and change type to a list
scrub_df = pd.read_csv('/Users/jrubin/google drive/Scrub/scrub 7.20.17.csv')
scrub_list = list([int(x) for x in scrub_df['Scrub']])

# Import list.csv as DataFrame
prospects_df = pd.read_csv('/Users/jrubin/google drive/shared lists for outbound/new/scrubTest.csv')
print("Number of rows in original prospect list.")
print(len(prospects_df['Business Phone']))

# Clean up: Drop unecessary columns, sort by phone number
columns_to_drop = ['MISC 1 2 3', 'Radius Rating', 'Radius ID', 'Campaign', 'Campaign (BAU or Imagine)', 'Lead_Source']
for column in columns_to_drop:
	prospects_df = prospects_df.drop(column, 1)
prospects_df.sort_values(by='Business Phone', inplace=True)

# Drop 2nd row of any duplicate phone numbers
prospects_df.drop_duplicates(subset=['Business Phone'], keep='first', inplace=True)
print("Number of prospects after duplicates prospects removed.")
print(len(prospects_df['Business Phone']))

# change strings of selected columns to all lowercase making it easier to ID strings to be removed
columns_to_lowercase= ['Company Name', 'Business Email', 'Contact Email', 'Website']
for column in columns_to_lowercase:
    prospects_df[column] = prospects_df[column].astype(str).str.lower()

# Strings to be removed from company names, e-mails, and urls/websites
# others not included but to be considered --> fidelity, citizens bank,
comp_names_to_drop = ['aaa insurance', 'aaa travel', 'american family', 'bail bond', 'bailbond', 'bb&t',
					  'bb & t', 'capitalone', 'capital one', 'chase bank', 'citigroup', 'morgan stanley'
                      'farm bureau', 'farmbureau', 'fifththird', 'fifth third', 'fifth-third', 'jpmorgan',
                      'jp morgan', 'pnc', 'state farm', 'statefarm', 'suntrust', 'sun trust']

emails_urls_to_drop = ['aaainsurance', 'americanfamily', 'bailbond', 'bail-bond', 'bail_bond', 'capitalone',
					   'citigroup', 'pncmortgage', '@pnc.com', 'suntrust', 
					   'statefarm', 'travelers.com', 'wellsfargo', 'wells-fargo']

# Drop items with specific company names, emails, and websites
names_dropped_df = prospects_df[~prospects_df['Company Name'].str.contains('|'.join(comp_names_to_drop))]
names_emails_dropped_df = names_dropped_df[~names_dropped_df['Business Email'].astype(str).str.contains('|'.join(emails_urls_to_drop))]
scrubbed_df = names_emails_dropped_df[~names_emails_dropped_df['Website'].astype(str).str.contains('|'.join(emails_urls_to_drop))]


print("Number of prospects after duplicates and specific companies dropped.")
print(len(scrubbed_df['Business Phone']))

# Link to API to get carrier and portability info
# secure link but pulls data from Neustar
url = 'https://securelink.com/ajax.php?action=lnpLookup&tnlist='

# Select column of phone numbers and change each to string for API calls
list_of_numbers = [str(x) for x  in list(scrubbed_df['Business Phone'])]

# Breaks up new list of strings of individual phone numbers 
# into a concatenated strings with, evenly splitting up to stay under
# maximum API call limit

# Max phone numbers at once seems to be just above 720 per request
list_of_number_strings = []
for i in range(0, len(list_of_numbers), 720):
	string_of_numbers = [i for i in list_of_numbers[i:i + 720]]
	list_of_number_strings.append(";".join(string_of_numbers))
	string_of_numbers = []

# Combines url list_of_number_strings and makes API call
# then packages details for each phone number in a single list
# and each of those lists into a list
final_carrier_port_list = []
for item in range(int(len(list_of_numbers)/720)):
	phone_number_json = requests.get(url + list_of_number_strings[item]).json()
	#print(phone_number_json)
	indiv_number_detail_list = []
	for i in phone_number_json['data']:
		indiv_number_detail_list.append(i['DID'])
		indiv_number_detail_list.append(i['Owner'])
		indiv_number_detail_list.append(i['Portability'])
		final_carrier_port_list .append(indiv_number_detail_list)
		indiv_number_detail_list = []

# turn list of lists into DataFrame
carrier_port_df = pd.DataFrame(final_carrier_port_list)

# Name columns (NOTE - Phone number column name matches other DF)
carrier_port_df.columns = ['Business Phone', 'Carrier', 'Portability']
scrubbed_data_added_df = scrubbed_df.merge(carrier_port_df, on='Business Phone')
print(scrubbed_data_added_df.head(10))
print(scrubbed_data_added_df.info())

scrubbed_data_added_df.to_csv('out.csv')
