# -*- coding: utf-8 -*-
''' Dependency Template Class to handle all aspects of the Positive Surprise offers '''
import pandas as pd
import os
import json
import logging
from datetime import date
import inspect
import templateservice.service as ts
from utils.snowflake import HalSnowflake as snow
import utils.sharepoint_utils as sp
import dependency_template.DependencyTemplateQueryUtil as DependencyTemplateQueryUtil
import utils.email_utils as eu
import utils.conn_utils as cu

class DependencyTemplate:

    def __init__(self, connection_name):
        """ Initialize the Dependency Template Module """
        self.snowcon = snow.HalSnowflake(connection_name)
        self.logger = logging.getLogger(os.getenv('LOG_ENV'))
        self.address = os.getenv("ADDRESS")
        self.port = os.getenv("PORT")
        self.spcon = sp.SharePoint(os.getenv("SHAREPOINT_BASE_URL_HAG"), os.getenv("SP_CLIENT_ID"), os.getenv("SP_CLIENT_SECRET"))
        self.dashboard = os.getenv("POWER_BI_DASHBOARD")
        self.scpcon = cu.create_scp_client_instance(cu.create_ssh_client_instance(os.getenv("SHIP_SERVER_SOURCE_HOST"), os.getenv("SHIP_SERVER_USERNAME"), os.getenv("SHIP_SERVER_PASSWORD")))
        self.today = date.today().strftime("(%m-%d-%y)")
        self.today_ship_time = date.today().strftime("%Y%m%d")
        self.mail_merge_options = {'page-height': '11in', 'page-width': '4.5in',}
        self.logger.debug(f"Initialized HAL Positive Surprise module with connection name {connection_name}")

    def get_subfolder_path(self, subfolder):
        ''' Get the absolute path for project files '''
        self.logger.debug(f"Getting the absolute path for subfolder {subfolder}")
        path = inspect.getfile(self.__class__)
        return f"{path[0:path.rfind('\\')+1]}{subfolder}"

    def load_hal_positive_surprise_data(self):
        """ Get the list of qualified guests """
        self.logger.debug(f"Fetching the daily process that runs and qualifies guests for positive surprises")
        qualified_guests = self.snowcon.query_data(DependencyTemplateQueryUtil.positive_surprise_execution(), multi_query=True)
        return qualified_guests
    
    def list_upcoming_voyages(self):
        """ Get a list of upcoming voyages to generate positive surprise fliers for"""
        self.logger.debug(f"Fetching a list of voyages")
        upcoming_voyages = self.snowcon.query_data(DependencyTemplateQueryUtil.upcoming_voyages())
        upcoming_voyages[['VOYAGE', 'SHIP_NAME', 'DEPARTMENT']] = upcoming_voyages[['VOYAGE', 'SHIP_NAME', 'DEPARTMENT']].apply(lambda x: x.str.upper())
        unique_voyage_list = upcoming_voyages[['SAIL_DATE','SHIP_NAME','DEPARTMENT']].drop_duplicates().sort_values(by='SHIP_NAME')
        return unique_voyage_list
        
    def load_voyage_department_data(self, ship_name, sail_date, department):
        """ Get a list qualified guests for a specific ship, sail date, and department """
        self.logger.debug(f"Fetching a list of qualified guests for {ship_name}, departing on {sail_date}, for {department} department ")
        return self.snowcon.query_data(DependencyTemplateQueryUtil.voyage_department_data(ship_name, sail_date, department))
    
    def load_voyage_spa_print_data(self, ship_name, sail_date):
        """ Get a list of qualified spa guests for a specific ship in preparation for the mail merge format """
        self.logger.debug(f"Fetching a list of qualified guests for {ship_name}, departing on {sail_date}, for Spa department in mail merge format ")
        return self.snowcon.query_data(DependencyTemplateQueryUtil.voyage_spa_print_data(ship_name, sail_date))
    
    def load_voyage_cabin_print_data(self, ship_name, sail_date):
        """ Get a list of qualified spa guests for a specific ship in preparation for the mail merge format """
        self.logger.debug(f"Fetching a list of qualified guests for {ship_name}, departing on {sail_date}, for Cabin department in mail merge format ")
        return self.snowcon.query_data(DependencyTemplateQueryUtil.voyage_cabin_print_data(ship_name, sail_date))
    
    def load_voyage_test_history_data(self, ship_name, sail_date):
        """ Get a list of qualified guests for a specific ship """
        self.logger.debug(f"Fetching a list of tested qualified guests for {ship_name}, departing on {sail_date}")
        return self.snowcon.query_data(DependencyTemplateQueryUtil.voyage_test_history_data(ship_name, sail_date))
    
    def load_voyage_push_notifications_data(self):
        """ Get a list of qualified guests for a specific ship for the app """
        self.logger.debug(f"Fetching a list of tested qualified guests for the app")
        return self.snowcon.query_data(DependencyTemplateQueryUtil.voyage_push_notifications_data())
 
    def export_data(self, df, filename, local_path, file_type='csv', export_to_sharepoint=0, sp_path=None):
        """ Handle the exporting of data frames. Supported export types are xlsx and csv """
        self.logger.debug(f"Exporting data to filename {filename}.{file_type}")
        #If local path does not exist, create it
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        local_file = f"{local_path}/{filename}.{file_type}" 
        if file_type.upper() == 'XLSX':
            df.to_excel(local_file, index=False)
        elif file_type.upper() == 'CSV':
            df.to_csv(local_file, index=False)

        if export_to_sharepoint == 1:
            self.spcon.push(local_file, sp_path)

    def load_spa_mail_merge_dictionary(self, df, ship_name):
        """ Generate the spa mail merge dictionary  """
        self.logger.debug(f"Loading the mail merge dictionary for {ship_name}")
        
        df = df.fillna('')
        mail_merge_dict =  {
                "ship": f'{ship_name}',
                "outputType": "PDF",
                "templateName": "HAL_PS_SPA_TEMPLATE_V1.html",
                "runtime": "local",
                "templateParms": json.loads(df.apply(
                    lambda x: {
                        'CABIN': x.CABIN,
                        'NAME': x.NAME,
                        'OFFER_GIFT_CARD': x.OFFER_GIFT_CARD,
                        'OFFER': x.OFFER,
                        'TC': x.TC,
                        'NAME': x.NAME,
                        'CABIN': x.CABIN,
                        'LOCATION': x.LOCATION,
                        'SHIP_NAME': x.SHIP_NAME,
                        'SAIL_DATE': x.SAIL_DATE,
                        'HOUSEKEEP_SECTION': x.HOUSEKEEP_SECTION,
                        'EXPIRATION_DATE': x.EXPIRATION_DATE,
                    }, axis=1).to_json(orient='records'))
            }               

        return mail_merge_dict
    
    def load_cabin_mail_merge_dictionary(self, df, ship_name):
        """ Generate the cabin mail merge dictionary """
        self.logger.debug(f"Loading the mail merge dictionary for {ship_name}")
        
        df = df.fillna('')
        mail_merge_dict =  {
                "ship": f'{ship_name}',
                "outputType": "PDF",
                "templateName": "HAL_PS_CABIN_TEMPLATE_V1.html",
                "runtime": "local",
                "templateParms": json.loads(df.apply(
                    lambda x: {
                        'CABIN': x.CABIN,
                        'NAME': x.NAME,
                        'HEADER': x.HEADER,
                        'OFFER2_GIFT_CARD': x.OFFER2_GIFT_CARD,
                        'OFFER2': x.OFFER2,
                        'TC2': x.TC2,
                        'EXPIRATION_DATE2': x.EXPIRATION_DATE2,
                        'NAME2': x.NAME2,
                        'CABIN2': x.CABIN2,
                        'LOCATION2': x.LOCATION2,
                        'SHIP_NAME2': x.SHIP_NAME2,
                        'SAIL_DATE2': x.SAIL_DATE2,
                        'HOUSEKEEP_SECTION2': x.HOUSEKEEP_SECTION2,
                        'OFFER1_GIFT_CARD': x.OFFER1_GIFT_CARD,
                        'OFFER1': x.OFFER1,
                        'TC1': x.TC1,
                        'EXPIRATION_DATE': x.EXPIRATION_DATE,
                        'NAME': x.NAME,
                        'CABIN': x.CABIN,
                        'LOCATION1': x.LOCATION1,
                        'SHIP_NAME': x.SHIP_NAME,
                        'SAIL_DATE': x.SAIL_DATE,
                        'HOUSEKEEP_SECTION': x.HOUSEKEEP_SECTION,
                    }, axis=1).to_json(orient='records'))
            }              

        return mail_merge_dict
    
    def generate_print_media(self, dictionary, local_path, file_name, options=None, export_to_sharepoint=False, sp_path=None):
        """ Generate the PDFs from the voyage dictionary """
        self.logger.debug(f"Generate the print media for {file_name}")
        filesCreated = ts.processLocal(dictionary, local_path, clean_folder=True, delete_single_files=True, merge_file_name=file_name, optionsDict=options, create_folder=True)
        
        if export_to_sharepoint:
            self.spcon.push(f"{local_path}/{file_name}", sp_path)

        return filesCreated
    
    def send_notifications(self, ship_name, sail_date, ship_data, email_list):
        """ Send notification to ships that Positive Surprise is complete """
        self.logger.debug(f"Sending notification for {ship_name} departing on {sail_date}")

        df_emails = pd.read_excel(f"{self.get_subfolder_path('communication')}/{email_list}")
        recipients = df_emails.loc[df_emails['Ship'] == ship_name, 'Recipients'].iat[0].split("; ")
        html_pieces = []
        for department, link in ship_data["links"].items():
            html_piece = f'''
            <html>
            <head></head>
            <body>
                <p>{department} ➔ <a href="{link}">{ship_name} {sail_date.strftime('%m-%d-%Y')}</a>
            </body>
            </html>
            '''
            html_pieces.append(html_piece)
        merged_html = '\n'.join(html_pieces)
        subject = f"Positive Surprise {ship_name.title()} {sail_date.strftime('(%m-%d-%Y)')}"
        self.send_email(subject, recipients, merged_html=merged_html)

    def send_email(self, subject, recipients, custom_html=None, merged_html=None):
        """ Send the email with default or custom HTML """
        sender_address = 'targetedmarketing@hollandamerica.com'
        receiver_address = recipients
        receiver_address_Cc = ['biautomations@hollandamerica.com']
        default_html  = f'''
        <html>
        <head></head>
        <body>
            <p>Hi,</p>
            <p>Attached, you will find links to Positive Surprise for the current sailing. Please be advised that access is limited to your department's link.</p>
            <p>{merged_html}</p>
            <p>Positive Surprise Guest List ➔ <a href="{self.dashboard}">DASHBOARD</a></p>
            <p><strong>Update:</strong> Introducing our new real-time data tool! Our Power BI dashboard consolidates all relevant department data, eliminating the hassle of SharePoint file sifting. Print files and mail merges stay on SharePoint. Access the dashboard through the unchanged link above. Analytics will be added later for key insights.</p>
            <br><em>To access the provided link, please ensure that you are logged into SharePoint online.</em></p>
            <p>If you have any questions or comments, please inform us.
            <br>We appreciate your help!
            <br>Targeted Marketing Team
            <p>Please note this is an automated message</p>
            <br><br>
        </body>
        </html>
        '''

        html_content = custom_html if custom_html else default_html

        email_utils = eu.EmailUtils(self.address, self.port)
        eu.EmailUtils.send_email(email_utils, subject, sender_address, receiver_address, receiver_address_Cc, html_content)

    def send_completion_email(self, df, today):
        """ Send a confirmation email after processing positive surprise automation """
        unique_ship_list = df[['SHIP_NAME']].drop_duplicates().sort_values(by='SHIP_NAME')
        ship_list_str = ', '.join(unique_ship_list['SHIP_NAME'].str.title().tolist())

        # Add "and" with a period after the last ship name
        if len(unique_ship_list) > 1:
            last_comma_index = ship_list_str.rfind(',')
            if last_comma_index != -1:
                ship_list_str = ship_list_str[:last_comma_index] + ", and" + ship_list_str[last_comma_index + 1:]
            else:
                ship_list_str = " and".join(ship_list_str.rsplit(",", 1))
            ship_list_str += "."

        subject = f"Positive Surprise Automation Pushed Data {today}"
        recipients = ['biautomations@hollandamerica.com']
        custom_html_content = f'''
        <html>
        <head></head>
        <body>
            <p>Hi,</p>
            <p>The Positive Surprise Automation executed successfully for the following ships: {ship_list_str}</p>
            <p>Please note this is an automated message</p>
        </body>
        </html>
        '''
        self.send_email(subject, recipients, custom_html=custom_html_content)

    def send_push_notifications(self, df, ship_time, local_path):
        """ Send push notifications by creating JSON files and transferring them """
        column_mapping = {
            'SHIP_NAME': 'shipName', 'SHIP_CODE': 'shipCode', 'VOYAGE': 'voyage',
            'SAIL_DATE': 'sailDate', 'RETURN_DATE': 'returnDate', 'FIRST_NAME': 'firstName',
            'LAST_NAME': 'lastName', 'BKNG_NBR': 'booking', 'PARTY_ID': 'party', 'FIDELIO_NUMBER': 'fidelioId',
            'LOYALTY_ID': 'loyaltyId', 'CABIN': 'cabin', 'OFFER_DEPT': 'offerDept', 'OFFER_CODE': 'offerCode',
            'OFFER': 'offer', 'OFFERTITLE': 'offerTitle', 'OFFER_MESSAGE': 'offerMessage',
            'CARD_TEMPLATE': 'cardTemplate', 'AMOUNT': 'amount', 'DELIVERY_DATE': 'deliveryDate',
            'DELIVERY_TIME': 'deliveryTime', 'EXPIRATION_DATE': 'expirationDate', 'TERMS': 'terms',
            'LOCATION': 'location'
        }

        df.rename(columns=column_mapping, inplace=True)
        ship_code_dict = df.groupby('shipName')['shipCode'].first().to_dict()

        ship_code_mapping = { 'NIEUW AMSTERDAM': 'NA', }

        for ship_name, ship_code in ship_code_dict.items():
            if ship_name in ship_code_mapping:
                ship_code_dict[ship_name] = ship_code_mapping[ship_name]

        for ship_name, ship_code in ship_code_dict.items():
            self.logger.debug(f"Processing: {ship_name}, Ship Code: {ship_code}")
            ship_data = df[df['shipName'] == ship_name]
            file_name = f'{local_path}/{ship_name}/notifications/{ship_code}_{ship_time}.json'
            ship_data_json = ship_data.to_json(orient='records')

            with open(file_name, 'w') as json_file:
                json.dump(json.loads(ship_data_json), json_file, indent=4)

            subject = f'{ship_code} Positive Surprise File QA Server'
            recipients = ['biautomations@hollandamerica.com']
            try:
                self.scpcon.put(file_name, remote_path='/approot/offers/upload')
                custom_message = f'This is to inform you the file has been copied successfully!'
                self.logger.debug(f'Succesffully copied the file')
                self.send_email(subject, recipients, self.create_html_content(custom_message))
            except Exception as e:
                custom_message = f'This is to inform you there has been an error copying the file! Error: {e}'
                self.logger.error(f'Error copying the file: {e}')
                self.send_email(subject, recipients, self.create_html_content(custom_message))

    def create_html_content(self, custom_message):
        return f'''
        <html>
        <head></head>
        <body>
            <p>Hi,</p>
            <p>{custom_message}</p>
            <p>Please note this is an automated message</p>
        </body>
        </html>
        '''