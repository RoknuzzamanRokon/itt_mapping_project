

def initialize_tracking_file(file_path, systemid_list):
    """
    Initializes the tracking file with all SystemIds if it doesn't already exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")


def read_tracking_file(file_path):
    """
    Reads the tracking file and returns a set of remaining SystemIds.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return {line.strip() for line in file.readlines()}


def write_tracking_file(file_path, remaining_ids):
    """
    Updates the tracking file with unprocessed SystemIds.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")


def append_to_cannot_find_file(file_path, systemid):
    """
    Appends the SystemId to the 'Cannot find any data' tracking file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")



def update_and_save_function(file_path):
    vervotech_id_list = read_tracking_file(file_path)
    
    if not vervotech_id_list:
        print(f"No Vervotech Id to process in {file_path}")
        return

    vervotech_id_list = list(vervotech_id_list)  # Convert set to list

    index = 0
    while index < len(vervotech_id_list):
        vervotech_id = vervotech_id_list[index]
        try:
            update_global_hotel_mapping(vervotech_id)
            
            # Remove the processed Vervotech Id from the list
            vervotech_id_list.pop(index)
            
            write_tracking_file(file_path, vervotech_id_list)

        except Exception as e:
            print(f"Error processing Vervotech {vervotech_id}: {e}")
            append_to_cannot_find_file("cannot_find_file.txt", vervotech_id)

file = "D:/Rokon/ittImapping_project/id_list_file.txt"
update_and_save_function(file)


