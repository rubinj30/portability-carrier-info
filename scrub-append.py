import pandas as pd
import requests
import sys


def import_file_to_scrub(prospects):
	"""import cold-calling prospects file"""
	prospects_df = pd.read_csv(prospects)
	print("Total prospects in original list:\n{}\n".format(len(prospects_df)))
	return prospects_df


def drop_and_edit_columns(prospects_file, drop, lowercase):
	"""Drop unnecessary columns"""
	for column in drop:
		prospects_file = prospects_file.drop(column, 1)
	for column in lowercase:
		prospects_file[column] = prospects_file[column].astype(str).str.lower()
	return prospects_file


def drop_prospects(prospects_file, comp_names_to_drop, emails_urls_to_drop):
    """Dropping unneeded columns and prospects that are duplicates or not ideal to contact"""
    prospects_file.drop_duplicates(subset=['Business Phone'], keep='first', inplace=True)
    print("Total prospects after duplicate TN's removed:\n{}\n".format(len(prospects_file)))
    names_dropped_df = prospects_file[~prospects_file['Company Name'].str.contains('|'.join(comp_names_to_drop))]
    names_emails_dropped_df = names_dropped_df[~names_dropped_df['Business Email'].astype(str).str.contains('|'.join(emails_urls_to_drop))]
    names_emails_dropped_df = names_emails_dropped_df[~names_emails_dropped_df['contact Email'].astype(str).str.contains('|'.join(emails_urls_to_drop))]
    scrubbed_df = names_emails_dropped_df[~names_emails_dropped_df['Website'].astype(str).str.contains('|'.join(emails_urls_to_drop))]
    scrubbed_df['Company Name'] = scrubbed_df['Company Name'].str.title()
    print("\nTotal prospects after company names removed based on name, email, or url:\n{}\n".format(len(scrubbed_df)))
    return scrubbed_df


def get_urls(prospects_file):
    """Get list of URLs ready for API calls. Around 720 phonenumbers allowed per API call"""
    url = 'https://aos-tools-01.kewr1.s.vonagenetworks.net/includes/EssentialsTools/Includes/Scripts/ajax.php?action=lnpLookup&tnlist='
    list_of_number_lists = []
    number_lists = []

    # sorting to make sure that the last list contains the highest numbers
    prospects_file.sort_values(by='Business Phone', inplace=True)

    # save phone number column as a list of strings
    original_list_of_numbers = [str(x) for x in list(prospects_file['Business Phone'])]
    
    for number in original_list_of_numbers:

    	# would it be easier to use a modulo to get the last list?
    	if original_list_of_numbers.index(number) == int(len(original_list_of_numbers)-1):
    		number_lists.append(number)
    		list_of_number_lists.append(number_lists)
    		
    		# breaks out if not it is the the last item in list
    		break
    	elif len(number_lists) > 720:  # seems to be max amount of phone numbers allowed per call. Can test with smaller amount for speed.
    		list_of_number_lists.append(number_lists)
    		number_lists = []
    		number_lists.append(number)
    	else:
    		number_lists.append(number)
    
    # since i am splitting the list randomly, i am checking the last list 
    print("Length of last list of numbers:\n")
    print(len(list_of_number_lists[-1]))

    # combining each subset of phone numbers into single string separated by ;
    # then adding each of these to the end of the url for API call
    list_of_api_calls = []
    for item in list_of_number_lists:
    	list_of_api_calls.append(url + ";".join(item))

    return list_of_api_calls


def get_carrier_port_info(list_of_api_calls):
	"""Make the API calls for JSON data, then extract required info for each number"""
	carrier_port_lists = []
	for call in list_of_api_calls:
		phone_number_json = requests.get(call).json()
		number_details = []
		for i in phone_number_json['data']:
			
			# slice for DID as a reference back to original list
			number_details.append(i['DID'])
			
			# current telephon service carrier
			# useful for analytics and additional insight for sales rep
			number_details.append(i['Owner'])
			
			# is the phone number portable to Vonage
			# useful so we can discard from cold-calling list
			number_details.append(i['Portability'])
			carrier_port_lists.append(number_details)
			number_details = []
	carrier_port_df = pd.DataFrame(carrier_port_lists)
	
	# rename columns (phone number needs to match the prospect dataframe)
	carrier_port_df.columns = ['Business Phone', 'Carrier', 'Portability']

	# change type of Business Phone column to int (needs to match prospect dataframe)
	carrier_port_df['Business Phone'] = pd.to_numeric(carrier_port_df['Business Phone'])
	print(carrier_port_df.head())
	return carrier_port_df


def return_final_output(scrubbed, carrier_and_porting):
	"""print scrubbed list, carrier/port info list, and combined list to 3 separate Excel spreadsheets"""
	
	scrubbed.to_csv("scrubbed_output_refactored.csv")
	print("Scrubbed Prospects\n{}\n".format(len(scrubbed)))
	carrier_and_porting.to_csv("carrier_output_refactored.csv")
	print("Carrier and Porting Count:\n{}\n".format(len(carrier_and_porting)))
	print(carrier_and_porting.head())
	
	# merge dataframes and then output to separate CSV file
	merge_scrub_carrier_df = scrubbed.merge(carrier_and_porting, on='Business Phone')
	print("merged DFs:\n")
	print(merge_scrub_carrier_df.head())
	merge_scrub_carrier_df.to_csv("merged_output_refactored.csv")



def main():
    """Main entry point for script"""
    prospects_df = import_file_to_scrub('/Users/jrubin/google drive/shared lists for outbound/new/scrubTest2.csv')

    columns_to_lowercase = ['Company Name', 'Website', 'Business Email', 'contact Email']
   
    columns_to_drop = ['MISC 1 2 3', 'Radius Rating', 'Radius ID', 'Campaign', 'Campaign (BAU or Imagine)', 'Lead_Source']
    
    prospects_df = drop_and_edit_columns(prospects_df, columns_to_drop, columns_to_lowercase)
    
    # added strings to scrub for and will continue to do so
    # can eventually get more complex and involve multiple columns as generalizing can scrub off good prospects
    comp_names_to_drop = ['aaa insurance', 'aaa travel', 'american family', 'bail bond', 'bailbond', 
                          'bb&t', 'bank of america', 'bb & t', 'bonding',
                          'capitalone', 'capital one', 'citifinancial',
                          'chase bank', 'citibank', 'citigroup', 'morgan stanley',
                          'farm bureau', 'farmbureau', 'fifththird', 'fifth third', 'fifth-third', 'jpmorgan',
                          'jp morgan', 'pnc', 'state farm', 'statefarm', 'suntrust', 'sun trust']
   
    emails_urls_to_drop = ['aaainsurance', '.aaa.com', 'aaatravel', 'amfam.com', 'americanfamily', 'bailbond', 
                          'bail-bond', 'bail_bond',
                          'bankofamerica.com', 'capitalone', 'citibank.com', '.citi.com', 'citigroup'
                          'farmbureau', 'pncmortgage', '@pnc.com', 'suntrust',
                          'statefarm', 'travelers.com', 'wellsfargo', 'wells-fargo']                  
    
    scrubbed_df = drop_prospects(prospects_df, comp_names_to_drop, emails_urls_to_drop)
    
    list_of_api_calls = get_urls(scrubbed_df)

    carrier_port_df = get_carrier_port_info(list_of_api_calls)

    return_final_output(scrubbed_df, carrier_port_df)

if __name__ == '__main__':
    sys.exit(main())
