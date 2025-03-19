import argparse

def list_of_strings(arg):
    if len(arg) == 0:
        return None
    else:
        return arg.split(',')

parser = argparse.ArgumentParser(description='Hal Positive Surprise Command Line Options')
parser.add_argument('-l', '--logconfig', type=str, default='../log_config.yaml', help='The logging configuration file')
parser.add_argument('-p', '--publish', type=int, default='1', help='Publish to ship and notify 1=Yes,0=No')
parser.add_argument('-a', '--app', type=int, default='1', help='Upload app push notifications to ship 1=Yes,0=No')
parser.add_argument('-o', '--outpath', type=str, default='../output/', help='The output path for hal positive surprise content')
parser.add_argument('-n', '--notifylist', type=str, default='ship_email_list.xlsx', help='The notification list to use test_email_list.xlsx or ship_email_list.xlsx')
parser.add_argument('-c', '--conn', type=str, default='hal_snowflake', help='Snowflake Connection')

args, unknown = parser.parse_known_args()

def getUnknownArguments():
    return unknown

def getArguments():
    return args

def getArgumentHelp():
    parser.print_help()