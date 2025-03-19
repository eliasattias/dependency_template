import dependency_template.DependencyTemplate as hps
import dependency_template.DependencyTemplateCLI as cli
import logging
import logging.config
import yaml
import os
from datetime import date


def setup_logging():
    with open(cli.getArguments().logconfig,  "r") as f:
        yaml_config = yaml.safe_load(f.read())
        logging.config.dictConfig(yaml_config)


def process_hal_positive_surprise_job():

    setup_logging()
    logger = logging.getLogger(os.getenv('LOG_ENV'))

    hal_positive_surprise = hps.HalPositiveSurprise(cli.getArguments().conn)

    logger.info(f"Starting Holland America Positive Surprise Process")

    qualified_guests = hal_positive_surprise.load_hal_positive_surprise_data()
    unique_voyage_list = hal_positive_surprise.list_upcoming_voyages()

    export_to_sp = cli.getArguments().publish
    push_notifications = cli.getArguments().app
    local_output = cli.getArguments().outpath
    email_list = cli.getArguments().notifylist

    # Dictionary to hold ship data
    ships = {}

    for index, row in unique_voyage_list.iterrows():
        sail_date = row['SAIL_DATE']
        ship_name = row['SHIP_NAME'] #Default upper
        department = row['DEPARTMENT'].lower()

        # Create a unique identifier for each ship instance combining ship name and sail date
        ship_key = (ship_name, sail_date)

        # If the ship name doesn't exist, create a new key,value pair.
        if ship_key not in ships:
            ships[ship_key] = {
                'departments': [],
                'links': {}
            }

        # Add the department to the corresponding ship if it's not in the list
        if department not in ships[ship_key]['departments']:
            ships[ship_key]['departments'].append(department)
    
    # Process each ship and department
    for (ship_name, sail_date), ship_data in ships.items():
        
        logger.info(f"Ship Name: {ship_name}")
        logger.info(f"Sail Date: {sail_date}")
        logger.info("Departments:")
        
        for department in ship_data['departments']:
            logger.info(f"- {department}")

            voyage_department_data = hal_positive_surprise.load_voyage_department_data(ship_name, sail_date, department.title())
            if not voyage_department_data.empty:
                hal_positive_surprise.export_data(voyage_department_data, f'{ship_name.title()}_{hal_positive_surprise.today}_{department}', f'{local_output}\\{ship_name}\\{department}\\', export_to_sharepoint=export_to_sp, sp_path=f'Positive Surprise/HAL/{ship_name}/{department.title()}/') #sp_path is non case sensitive
                # ship_data['links'][f"{'Credit Card' if department == 'CC' else department.title()} Department"] = f"{sharepoint_path}Positive%20Surprise/{ship_name}/{department}/{file_path}?web=1"

            if department == 'spa':
                voyage_spa_print_data = hal_positive_surprise.load_voyage_spa_print_data(ship_name, sail_date)
                if not voyage_spa_print_data.empty:
                    hal_positive_surprise.export_data(voyage_spa_print_data, f'{ship_name.title()}_{hal_positive_surprise.today}_Spa_Mail_Merge', f'{local_output}\\{ship_name}\\gsm\\', export_to_sharepoint=export_to_sp, sp_path=f'Positive Surprise/HAL/{ship_name}/GSM/')
                    #ship_data['links']['Spa Mail Merge'] = f'{sharepoint_path}Positive%20Surprise/{ship_name}/GSM/{file_path}?web=1'
                    
                    hal_positive_surprise.generate_print_media(hal_positive_surprise.load_spa_mail_merge_dictionary(voyage_spa_print_data, ship_name), f'{local_output}\\{ship_name}\\mm\\', f'{ship_name.title()}_Spa_{hal_positive_surprise.today}_PrintFile.pdf', hal_positive_surprise.mail_merge_options, export_to_sharepoint=export_to_sp, sp_path=f'Positive Surprise/HAL/{ship_name}/MM/')
                    ship_data['links']['Spa Print File'] = f'{os.getenv("SHAREPOINT_BASE_URL_HAG")}/Positive Surprise/HAL/{ship_name}/MM/{ship_name.title()}_Spa_{hal_positive_surprise.today}_PrintFile.pdf?web=1'
        
        voyage_cabin_print_data = hal_positive_surprise.load_voyage_cabin_print_data(ship_name, sail_date)
        if not voyage_cabin_print_data.empty:
                    hal_positive_surprise.export_data(voyage_cabin_print_data, f'{ship_name.title()}_{hal_positive_surprise.today}_Cabin_Mail_Merge', f'{local_output}\\{ship_name}\\gsm\\', export_to_sharepoint=export_to_sp, sp_path=f'Positive Surprise/HAL/{ship_name}/GSM/')
                    #ship_data['links']['Cabin Mail Merge'] = f'{sharepoint_path}Positive%20Surprise/{ship_name}/GSM/{file_path}?web=1'
                    
                    hal_positive_surprise.generate_print_media(hal_positive_surprise.load_cabin_mail_merge_dictionary(voyage_cabin_print_data, ship_name), f'{local_output}\\{ship_name}\\mm\\', f'{ship_name.title()}_Cabin_{hal_positive_surprise.today}_PrintFile.pdf', hal_positive_surprise.mail_merge_options, export_to_sharepoint=export_to_sp, sp_path=f'Positive Surprise/HAL/{ship_name}/MM/')
                    ship_data['links']['Cabin Print File'] = f'{os.getenv("SHAREPOINT_BASE_URL_HAG")}/Positive Surprise/HAL/{ship_name}/MM/{ship_name.title()}_Cabin_{hal_positive_surprise.today}_PrintFile.pdf?web=1'
                    
        voyage_test_history_data = hal_positive_surprise.load_voyage_test_history_data(ship_name, sail_date)
        if not voyage_test_history_data.empty:
                hal_positive_surprise.export_data(voyage_test_history_data, f'{ship_name.title()}_{hal_positive_surprise.today}_GSM', f'{local_output}\\{ship_name}\\gsm\\', export_to_sharepoint=export_to_sp, sp_path=f'Positive Surprise/HAL/{ship_name}/GSM/')
                #ship_data['links']['Guest Service Manager'] = f"{sharepoint_path}Positive%20Surprise/{ship_name}/GSM/{file_path}?web=1"
    
    for (ship_name, sail_date), ship_data in ships.items():
        hal_positive_surprise.send_notifications(ship_name, sail_date, ship_data, email_list)
    
    hal_positive_surprise.send_completion_email(unique_voyage_list, hal_positive_surprise.today)

    if push_notifications == 1:
        logger.info(f"Sending Push Notifications to the ships")
        voyage_push_notifications_data = hal_positive_surprise.load_voyage_push_notifications_data()
        hal_positive_surprise.send_push_notifications(voyage_push_notifications_data,hal_positive_surprise.today_ship_time, f'{local_output}')
    
    logger.info(f"Ending Holland America Positive Surprise Process")
