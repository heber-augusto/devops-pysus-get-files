import os
import argparse

if __name__ == '__main__':
    # Construct the argument parser
    ap = argparse.ArgumentParser()

    # Add the arguments to the parser
    ap.add_argument("-c", "--config_path", required=True,
    help="input file")
    ap.add_argument("-t", "--team_drive_id", required=True,
    help="output file")
    args = vars(ap.parse_args())


    with open(args['config_path'], 'r') as fd:
        config_content = fd.readlines()
    
    with open(args['config_path'], 'w') as fd2:
        for content in config_content:
            if content.find('team_drive_id=') >= 0:
                fd2.write(f"team_drive_id={args['team_drive_id']}\n")
            else:
                fd2.write(content)
