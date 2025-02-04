def update_global_hotel_mapping(supplier, unica_id):
    print(f"DEBUG: Starting update for {unica_id}")
    
    hotel_data = get_a_column_info(supplier, unica_id)
    if hotel_data is None:
        print(f"Skipping update for {unica_id} because hotel_data is None")
        return

    gata_api = GataAPI()
    giata_id, provider_records = gata_api.get_data(supplier, hotel_data)

    if giata_id is None or provider_records is None:
        print(f"Skipping update for {unica_id} due to missing giata_id or provider_records")
        return

    provider_mappings = {
        "hotelbeds": ["hotelbeds", "hotelbeds_a", "hotelbeds_b", "hotelbeds_c", "hotelbeds_d", "hotelbeds_e"],
        "agoda": ["agoda", "agoda_a", "agoda_b", "agoda_c", "agoda_d", "agoda_e"],
        "tbo": ["tbohotel", "tbohotel_a", "tbohotel_b", "tbohotel_c", "tbohotel_d", "tbohotel_e"],
        "ean": ["ean", "ean_a", "ean_b", "ean_c", "ean_d", "ean_e"],
        "mgholiday": ["mgholiday", "mgholiday_a", "mgholiday_b", "mgholiday_c", "mgholiday_d", "mgholiday_e"],
        "restel": ["restel", "restel_a", "restel_b", "restel_c", "restel_d", "restel_e"],
        "stuba": ["stuba", "stuba_a", "stuba_b", "stuba_c", "stuba_d", "stuba_e"],
    }

    values_to_update = {col: None for cols in provider_mappings.values() for col in cols}

    for provider_code, ids in provider_records.items():
        if provider_code in provider_mappings:
            provider_columns = provider_mappings[provider_code]
            for i, provider_id in enumerate(ids):
                if i >= len(provider_columns):  # Avoid index out of range
                    break  

                column_name = provider_columns[i]
                current_val = getattr(existing_record, column_name, None)

                # Ensure we don't store duplicates
                if current_val is None or current_val.strip() == "":
                    values_to_update[column_name] = provider_id
                elif str(provider_id) not in str(current_val).split():
                    for col in provider_columns:
                        if not getattr(existing_record, col, None):
                            values_to_update[col] = provider_id
                            break

    try:
        existing_record = session.query(global_hotel_mapping).filter(global_hotel_mapping.c[supplier] == unica_id).first()
    except Exception as e:
        print(f"Error fetching existing record for {unica_id}: {e}")
        return

    final_update_values = {}
    if existing_record and not existing_record.GiataCode:
        final_update_values["GiataCode"] = giata_id
        final_update_values["mapStatus"] = "G-Done"

    for column, value in values_to_update.items():
        if value is not None:
            final_update_values[column] = value

    if final_update_values:
        query = update(global_hotel_mapping).where(global_hotel_mapping.c[supplier] == unica_id).values(**final_update_values)
        
        try:
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    session.execute(query)
                    session.commit()
                    print(f"Successful update: {unica_id}")
                    break 
                except Exception as e:
                    print(f"Commit attempt {attempt + 1} failed for {unica_id}: {e}")
                    session.rollback()
                    if attempt < max_attempts - 1:
                        time.sleep(2)
                    else:
                        print(f"Failed to commit after {max_attempts} attempts for {unica_id}")
        except Exception as e:
            print(f"Error during update execution for {unica_id}: {e}")
            session.rollback()
    else:
        print(f"No changes made for: {unica_id}")
