from influxdb import InfluxDBClient
import pandas as pd
import pytz   #  HERE THE START AND END TIME HAS BEEN CHANGED AND THE TIME GAP WILL BE CALCULATED 

def get_frequency(client, vehicle_id, start_date, end_date, start_time, end_time, measurement, can_id_fields, batch_size=100000):
    frequency_dict = {can_id: {field: 0 for field in fields} for can_id, fields in can_id_fields.items()}
    local_tz = pytz.timezone('Asia/Kolkata')
    
    # Combine date and time, and then convert to UTC
    current_time = pd.Timestamp(f'{start_date} {start_time}').tz_localize(local_tz).tz_convert('UTC')
    final_end_time = pd.Timestamp(f'{end_date} {end_time}').tz_localize(local_tz).tz_convert('UTC')
    print(current_time,final_end_time)
    total_gap_duration = pd.Timedelta(0)
    all_data = []
    
    # Iterate over each hour to avoid fetching too much data at once
    while current_time < final_end_time:
        next_time = current_time + pd.Timedelta(minutes=30) #  hours=1        based on  the 30 minutes ------------------------ change the thing 
        
        query = f"""
        SELECT *
        FROM {measurement}
        WHERE vehicle_id='{vehicle_id}' AND time >= '{current_time.isoformat()}' AND time < '{next_time.isoformat()}'
        ORDER BY time ASC LIMIT {batch_size}
        """

        # query = f"""
        # SELECT *
        # FROM {measurement}
        # WHERE time >= '{current_time.isoformat()}' AND time < '{next_time.isoformat()}'
        # ORDER BY time ASC LIMIT {batch_size}
        # """
        # print(query)
        results = client.query(query)
        data = pd.DataFrame(list(results.get_points()))
        # print(data)
        if not data.empty:
            # Convert 'time' to datetime, ensuring UTC format with ISO8601
            data['epoch'] = pd.to_datetime(data['time'], format='ISO8601', utc=True).astype(int) // 10**9  # Convert to epoch time
            data['time_diff'] = data['epoch'].diff()
            # print(data['time_diff'])
            # Check for gaps greater than 10 minutes (600 seconds)   default is 600 
            gaps = data[data['time_diff'] > 4]   # the gap condition kept as 600 second (10 minutes)
            # print(data[data['time_diff')
            if not gaps.empty:
                total_gap_duration += pd.Timedelta(seconds=gaps['time_diff'].sum() - 4 * len(gaps))
                # print(total_gap_duration)
            all_data.append(data)
        
        current_time = next_time

    print('total_',total_gap_duration)
    # Calculate total effective time considering gaps
    effective_time = (final_end_time - pd.Timestamp(f'{start_date} {start_time}').tz_localize(local_tz).tz_convert('UTC')) - total_gap_duration
    print((final_end_time - pd.Timestamp(f'{start_date} {start_time}').tz_localize(local_tz).tz_convert('UTC')))
    if all_data:
        concatenated_data = pd.concat(all_data, ignore_index=True)
        # print(concatenated_data)
        concatenated_data = pd.concat(all_data, ignore_index=False)
        # print(concatenated_data)
        for can_id, fields in can_id_fields.items():
            for field in fields:
                frequency_dict[can_id][field] = concatenated_data[field].count()
    
    return frequency_dict, effective_time

def main():
    # Connect to InfluxDB
    # client = InfluxDBClient(host='104.154.190.81', port=15086, username='boson_guest', password='boson_32%comp', database='bosondb_modern')
    client = InfluxDBClient(host='104.154.190.81', port=15086, username='boson_hmi', password='hmi@boson76$', database='HMI_test')         # for the testbeanch -------------------------
    # client = InfluxDBClient(host='10.10.0.79', port=8086, username='admin', password='admin', database='HMI_test')       #  NRU BD ------------------------
    vehicle_id = 'VT-Box-T1'   #   MD9GBUE25DC341064    VT-Box-T1   OfficeHmi   VT-Box-T6'   MD9GBUE2XDC341030  VT-Box-BD
    start_date = '2025-12-08'
    end_date = '2025-12-08'
    start_time = '11:00:00' 
    end_time = '14:20:00' 
     
    print(vehicle_id)
    measurements_data = {
    'bms_state_limits': {
        'CAN_ID_1': ['battery_dischrg_status', 'battery_dischrg_limit', 'battery_chrg_limit', 'chrg_interlock_state'],
        'CAN_ID_2': ['chrg_status', 'chrgr_highest_voltage', 'chrgr_highest_current'],
        'CAN_ID_3': ['j1772_current_limit', 'j1772_plug_state']
    },
    'bms_battery_health': {
        'CAN_ID_1': ['battery_adaptive_total_capacity', 'battery_soh', 'battery_total_cycles']
    },
    'bms_battery_weather': {
        'CAN_ID_1': ['battery_soc', 'battery_amp_hours', 'battery_highest_temp', 'highest_temp_thermistor_id', 'battery_lowest_temp', 'lowest_temp_thermistor_id', 'battery_average_temp'],
        'CAN_ID_2': ['bms_temp', 'battery_adaptive_soc', 'battery_adaptive_amp_hours', 'auxilary_battery_voltage']
    },
    'controller_motor_usage_1_REAR': {
        'CAN_ID_1': ['motorcontroller_1_average_motor_stator_current', 'motorcontroller_1_average_motor_phase_voltage', 'motorcontroller_1_motor_actual_torque'],
        'CAN_ID_2': ['motorcontroller_1_motor_rpm'],
        'CAN_ID_3': ['motorcontroller_1_calculated_battery_current', 'motorcontroller_1_controller_capacitor_voltage', 'motorcontroller_1_throttle_input']
    },
    'controller_motor_usage_2_FRONT': {
        'CAN_ID_1': ['motorcontroller_2_average_motor_stator_current', 'motorcontroller_2_average_motor_phase_voltage', 'motorcontroller_2_motor_actual_torque'],
        'CAN_ID_2': ['motorcontroller_2_motor_rpm'],
        'CAN_ID_3': ['motorcontroller_2_calculated_battery_current', 'motorcontroller_2_controller_capacitor_voltage']
    },
    'bms_battery_usage': {
        'CAN_ID_1': ['battery_current', 'battery_voltage']
    },
    'charger_state_limits': {
        'CAN_ID_1': ['chrgr_output_voltage', 'chrgr_output_current', 'chrgr_temp', 'chrgr_temp_error_status', 'chrgr_input_voltage_error_status', 'chrgr_communication_error_status']
    },
    'bms_fault_and_safety_state': {
        'CAN_ID_1': [
            "cell_asic_fault", "cell_balancing_active_non_fail_safe_mode", "cell_balancing_stuck_off_fault",
            "chrg_current_limit_reduced_due_to_alternate_current_limit", "chrg_current_limit_reduced_due_to_chrg_latch",
            "chrg_current_limit_reduced_due_to_high_cell_resistance", "chrg_current_limit_reduced_due_to_high_cell_voltage",
            "chrg_current_limit_reduced_due_to_high_pack_voltage", "chrg_current_limit_reduced_due_to_high_soc",
            "chrg_current_limit_reduced_due_to_temp", "chrg_interlock_fail_safe_active", "chrg_limit_enforcement_fault",
            "chrgr_safety_relay_fault", "current_fail_safe_active", "current_sensor_fault",
            "dischrg_current_limit_reduced_due_to_communication_fail_safe", "dischrg_current_limit_reduced_due_to_high_cell_resistance",
            "dischrg_current_limit_reduced_due_to_low_cell_voltage", "dischrg_current_limit_reduced_due_to_low_pack_voltage",
            "dischrg_current_limit_reduced_due_to_low_soc", "dischrg_current_limit_reduced_due_to_temp",
            "dischrg_current_limit_reduced_due_to_voltage_fail_safe", "external_communication_fault", "fan_monitor_fault",
            "high_voltage_isolation_fault", "highest_cell_voltage_over_5v_fault", "highest_cell_voltage_too_high_fault",
            "input_power_supply_fail_safe_active", "input_power_supply_fault", "internal_communication_fault",
            "internal_hardware_fault", "internal_heatsink_thermistor_fault", "internal_software_fault",
            "low_cell_voltage_fault", "lowest_cell_voltage_too_low_fault", "open_wiring_fault", "pack_too_hot_fault",
            "redundant_power_supply_fault", "relay_fail_safe_active", "thermistor_b_value_table_invalid",
            "thermistor_fault", "voltage_fail_safe_active", "weak_cell_fault", "weak_pack_fault"
        ]
    },
    'bms_cell_values': {
        'CAN_ID_1': ['cell_id', 'cell_voltage', 'cell_internal_resistance', 'cell_open_voltage', 'cell_shunt_status']
    },
    'controller_motor_status_1_REAR': {
        'CAN_ID_1': ['motorcontroller_1_controller_temp', 'motorcontroller_1_distance_travelled', 'motorcontroller_1_motor_temp', 'motorcontroller_1_seat_switch']
    },
    'controller_status_1_REAR': {
        'CAN_ID_1': ['motorcontroller_1_controller_status']
    },
    'controller_fault_status_1_REAR': {
        'CAN_ID_1': ['motorcontroller_1_fault_code']
    },
    'controller_motor_status_2_FRONT': {
        'CAN_ID_1': ['motorcontroller_2_motor_temp', 'motorcontroller_2_controller_temp']
    },
    'controller_status_2_FRONT': {
        'CAN_ID_1': ['motorcontroller_2_controller_status']
    },
    'DC_DC_Conv_fault_status': {
        'CAN_ID_1': ['Over_temperature_shutdown', 'communication_fault_alarm', 'input_overvoltage_alarm', 'input_undervoltage_alarm', 'internal_fault_alarm', 'output_overcurrent_alarm', 'output_overvoltage_alarm', 'output_shortcircuit_protection', 'output_undervoltage_alarm']
    },
    'DC_DC_Conv_OutputCurrent': {
        'CAN_ID_1': ['12Vconv_Outputcurrent']
    },
    'DC_DC_conv_OutputVoltage': {
        'CAN_ID_1': ['12Vconv_Outputvoltage']
    },
    'DC_DC_conv_workingstatus': {
        'CAN_ID_1': ['working_status']
    },
    'DC_DC_conv_temperature': {
        'CAN_ID_1': ['DC_DC_converter_inner_temperature']
    }
    ,
    'version_details':{
        # 'CAN_ID_1':['mew_bootloader_firmware_version'],
        'CAN_ID_2':['mew_hardware_version'],
        'CAN_ID_3':['mew_firmware_version'],
        'CAN_ID_4':['BMS_Profile_Checksum','BMS_Firmware_Version'],
        'CAN_ID_4':['MC1_Profile_Checksum','MC2_Profile_Checksum'],
        'CAN_ID_5':['BMS_Serial_Number'],
        'CAN_ID_6':['MC1_Serial_Number','MC2_Serial_Number'],
        'CAN_ID_7':['MC1_Firmware_Number'],
        'CAN_ID_8':['MC2_Firmware_Number'],
        'CAN_ID_9':['VIN_number']
    },
    'orion_bms_fault':{
        'CAN_ID_1':['7E3_byte1','7E3_byte2','7E3_byte3','7E3_byte4','7E3_byte5','7E3_byte6','7E3_byte7','7E3_byte8'],
        'CAN_ID_2':['7EB_byte1','7EB_byte2','7EB_byte3','7EB_byte4','7EB_byte5','7EB_byte6','7EB_byte7','7EB_byte8']
    },
    'EPAS_oe_demand_state_limits':{
        '314':['control_mode','neutral_pos','demand_angular_velocity','demand_steering','neutral_pos','}control_mode']
    },
    'EPAS_oe_response_state_limits':{
        '18f':['ECUState','ECUTemperature','angular_velocity','fault_enumeration','steering_angle']
    },    
    }

    measurements_data={'Zekrom_BMS_01': {'181': ['BatteryDischargeLimit', 'BatteryChargeLimit', 'BMS_DischargeState', 'BMS_ChargeInterlock', 'BMS_MutiPrpsEnbl', 'BMS_ReadyPower', 'BMS_ChargePower', 'BMS_ChargeRelay']}, 
                       'Zekrom_BMS_02': {'182': ['BMS_BatteryCurrent', 'BMS_BatteryVoltage']}, 
                       'Zekrom_BMS_03': {'183': ['BMS_BatterySOC', 'BMS_BatteryAmphours', 'BMS_BatteryHighestTemperature', 'BMS_HighestTempThermistorID', 'BMS_BatteryLowestTemperature', 'BMS_LowestTempThermistorID', 'BMS_BatteryAverageTemperature']}, 
                       'Zekrom_BMS_04': {'184': ['BMS_Temperature', 'BMS_BatteryAdaptiveSOC', 'BMS_BatteryAdaptiveAmphours', 'BMS_AuxilaryBatteryVoltage']},
                          'Zekrom_BMS_05': {'185': ['BMS_VoltageFailsafe', 'BMS_CurrentFailsafe', 'BMS_RelayFailsafe', 'BMS_CellbalancingFailsafe', 'BMS_ChargeInterlockFault', 'BMS_ThermistorBvalueTableInvalid', 'BMS_InputPowerSupplyFailsafe', 'BMS_DischargeLimitEnforcementEr', 'BMS_ChargerSafetyRelayEr', 'BMS_InternalHardwareEr', 'BMS_InternalHeatSinkThermistorEr', 'BMS_InternalSoftwareFaultEr', 'BMS_HighestCellVoltageEr', 'BMS_LowestCellVoltageEr', 'BMS_PackTooHotEr', 'BMS_InternalCommunicationEr', 'BMS_CellBalancingEr', 'BMS_WeakCellFaultEr', 'BMS_LowCellVtgFaultEr', 'BMS_OpenWiringFaultEr', 'BMS_CurrentSensorFaultEr', 'BMS_P0A0D_Over5vFaultEr', 'BMS_CellASICFaultEr', 'BMS_WeakPackFaultEr', 'BMS_FanMonitorEr', 'BMS_ThermistorFaultEr', 'BMS_ExtrnalCommunicationEr', 'BMS_RedudantPowerSuplyEr', 'BMS_HighVoltageIsolationEr', 'BMS_InputPowerSuplyFaultEr', 'BMS_ChargeLimitEnforcemeEr', 'BMS__DCLReducedLowSOC', 'BMS_DCLReducedHighCellResis', 'BMS_DCLReducedTemperature', 'BMS_DCLReducedLowCellVltg', 'BMS_DCLReducedLowPackVltg', 'BMS_DCL_CCL_ReducedVltgFailsafe', 'BMS_DCL_CCL_ReducedCommFailsafe', 'BMS_CCLReducedHighSOC', 'BMS_CCLReducedHighCellResistance', 'BMS_CCLReducedTemperature', 'BMS_CCLReducedHighCellVltg', 'BMS_CCLReducedHighPackVltg', 'BMS_CCLReducedChargerLatch', 'BMS_CCLReducedAltrntCurrentLmt']}, 
                          'Zekrom_BMS_06': {'186': ['BMS_J1772CurrentLimit', 'BMS_J1772PlugState', 'BMS_BatteryAdaptiveTotalCapacity', 'BMS_BatteryTotalCycles', 'BMS_BatterySOH']}, 
                          'Zekrom_BMS_07': {'187': ['BMS_CELL_ID', 'BMS_CHECKSUM']}, 'Zekrom_DCDC_01': {'9800e5f5': ['Conv12V_OtptOvrTempShtdwn', 'Conv12V_OtptOvrCurrent', 'Conv12V_OtptOvrVltg', 'Conv12V_OtptUndrVltg', 'Conv12V_InptOvrVltg', 'Conv12V_InptUndrVltg', 'Conv12V_OtptShrtCrct', 'Conv12V_IntrnlFlt', 'Conv12V_CommFlt', 'Conv12V_OutputCurrent', 'Conv12V_OutputVoltage', 'Conv12V_WorkingStatus']}, 'Zekrom_DCDC_02': {'9800f5e5': ['Conv12V_StatusCommand', 'Conv12V_HighestVoltage', 'Conv12V_HighestCurrent']}, 'Zekrom_DCE_Epas_01': {'290': ['EPS_RANE_SteeringTorque', 'EPS_RANE_MotorDuty', 'EPS_RANE_MotorCurrent', 'EPS_RANE_BatteryVoltage', 'EPS_RANE_SteeringAngle', 'EPS_RANE_Temperature']}, 'Zekrom_DCE_Epas_02': {'292': ['EPS_RANE_DriveMode', 'EPS_RANE_ErrorState']}, 'Zekrom_DCE_Epas_03': {'298': ['EPS_RANE_TargetMode', 'EPS_RANE_TargetAngle', 'EPS_RANE_MaxTorqueGain', 'EPS_RANE_MaxSteeringRate']}, 'Zekrom_HUD_01': {'341': ['HUD_Soc', 'HUD_SoC_Graph', 'HUD_TTC_Hr', 'HUD_TTC_Min']}, 'Zekrom_HUD_02': {'342': ['HUD_MotorTemperature']}, 'Zekrom_HUD_03': {'343': ['HUD_Battery_Temperature', 'HUD_Motor_Over_Temperature', 'HUD_Buzzer', 'HUD_RTC_Hr', 'HUD_RTC_Min', 'HUD_Regeneration', 'HUD_ParkBrake', 'HUD_MalfunctionIdicator', 'HUD_HelmetIcon', 'HUD_ServiceRemainder', 'HUD_Forward', 'HUD_Reverse', 'HUD_ECO', 'HUD_Power', 'HUD_SportMode', 'HUD_MotorFailure', 'HUD_Neutral', 'HUD_Charge_Icon', 'HUD_Turbo', 'HUD_SideStand', 'HUD_ErrorCode_1', 'HUD_ErrorCode_2']}, 'Zekrom_MC_01': {'201': ['mcRear_AvrgMtrStatorCurrent', 'mcRear_AvrgMtrPhaseVoltage', 'mcRear_MaximumTorque', 'mcRear_MtrActualTorque']}, 'Zekrom_MC_02': {'202': ['mcRear_TargetSpeed', 'mcRear_MotorRPM']}, 'Zekrom_MC_03': {'203': ['mcRear_BatteryCurrent', 'mcRear_CapacitorVoltage', 'mcRear_ThrottleInput']}, 'Zekrom_MC_04': {'204': ['mcRear_MotorTemperature', 'mcRear_Temperature', 'mcRear_DistanceTravelled', 'mcRear_ForwardSwitch', 'mcRear_ReverseSwitch', 'mcRear_SeatSwitch', 'mcRear_FootbrakeSwitch']}, 'Zekrom_MC_05': {'205': ['mcRear_ControlWord', 'mcRear_TargetVelocity', 'mcRear_MaxTorque']}, 'Zekrom_MC_06': {'701': ['mcRear_Status']}, 'Zekrom_MC_07': {'80': ['mcRear_SyncMessage']}, 'Zekrom_MC_08': {'81': ['mcRear_ErrorCode', 'mcRear_ErrorRegistor', 'mcRear_FaultCode']},
                        'Zekrom_MC_21': {'221': ['mcFront_AvrgMtrStatorCurrent', 'mcFront_AvrgMtrPhaseVoltage', 'mcFront_MaximumTorque', 'mcFront_MotorActualTorque']}, 
                        'Zekrom_MC_22': {'222': ['mcFront_TargetSpeed', 'mcFront_MotorRPM']},
                          'Zekrom_MC_23': {'223': ['mcFront_BatteryCurrent', 'mcFront_CapacitorVoltage']}, 'Zekrom_MC_24': {'224': ['mcFront_MotorTemperature', 'mcFront_Temperature']}, 'Zekrom_MC_25': {'225': ['mcFront_StatusWord', 'mcFront_ActualVelocity', 'mcFront_ActualTorque']},
                            'Zekrom_MC_26': {'702': ['mcFront_Status']}, 'Zekrom_MC_28': {'82': ['mcFront_ErrorCode', 'mcFront_ErrorRegistor', 'mcFront_FaultCode']}, 'Zekrom_NRU_01': {'510': ['OBC_TargetRPM', 'OBC_ForwardFlag', 'OBC_ReverseFlag', 'OBC_FootbrakeFlag', 'OBC_HeadLampFlag', 'OBC_RightIndicatorFlag', 'OBC_LeftIndicatorFlag', 'OBC_HornFlag']},
                              'Zekrom_OBC_01': {'9806e5f4': ['Chrgr_StatusCommand', 'Chrgr_Mode']}, 'Zekrom_OBC_02': {'98ff50e5': ['Chrgr_HWStatus', 'Chrgr_ThermalStatus', 'Chrgr_InputVoltageStatus', 'Chrgr_StartStatus', 'Chrgr_CommStatus']}, 
                          'Zekrom_OE_Epas_01': {'18f': ['EPS_OE_CommFaultStatus','EPS_OE_CurrentAngleValue','EPS_OE_CurrentAnglrVelocity','EPS_OE_FaultStatus','EPS_OE_FaultStatusLvl1','EPS_OE_FaultStatusLvl2','EPS_OE_FaultStatusLvl3','EPS_OE_MdlPosCalbStat','EPS_OE_Mode','EPS_OE_Temperature']}, 
                          'Zekrom_VCU_01': {'188': ['ThrottleInput', 'ForwardSwitch', 'ReverseSwitch', 'FootBrake', 'HandBrake']},'Zekrom_VCU_03': {'420': ['BMS_Profile_Checksum']},'Zekrom_VCU_05': {'422': ['MC_Front_Profile_Checksum','MC_Rear_Profile_Checksum']}, 'Zekrom_VCU_06': {'423': ['MC_Rear_Serial_Number']},
                          'Zekrom_VCU_08': {'425': ['MC_rear_Firmware_Num_stage2']},'Zekrom_VCU_10': {'427': ['MC_Front_Firmware_Num_Stage2']}, 
                          'Zekrom_VCU_11': {'440': ['Major_Version_Hardware','Minor_Version_Hardware','Patch_Version_Hardware']}, 'Zekrom_VCU_12': {'441': ['Major_Version_Firmware','Minor_Version_Firmware','Patch_Version_Firmware']}, 
                          'Zekrom_VCU_13': {'55': ['Major_Versio_Bootloader_Firmware','Minor_Versio_Bootloader_Firmware','Patch_Versio_Bootloader_Firmware']}, 
                          'Zekrom_VCU_14': {'442': ['VIN_Num_Stage1_Identifier']}, 'Zekrom_VCU_15': {'443': ['VIN_Num_Stage2_Identier']}, 'Zekrom_VCU_16': {'444': ['VIN_Num_Stage3_12to16_Bytes','VIN_Num_Stage3_Identifier']}}

    for i,j in measurements_data.items():
        frequency_dict, effective_time = get_frequency(client, vehicle_id, start_date, end_date, start_time, end_time, i, j)
        print('----------------------------------',i,'--------------------------------------------------')
        print(f"Effective Time (considering gaps): {effective_time}")
    
        for can_id, frequency in frequency_dict.items():
            print(f"Frequency for {can_id}:")
            for field, count in frequency.items():
                print(f"{field}: {count}")
            print("\n")


    
    # print(measurements_data)
    # frequency_dict, effective_time = get_frequency(client, vehicle_id, start_date, end_date, start_time, end_time, measurements_data, can_id_fields)
    
    # print(f"Effective Time (considering gaps): {effective_time}")

    # for can_id, frequency in frequency_dict.items():
    #     print(f"Frequency for {can_id}:")
    #     for field, count in frequency.items():
    #         print(f"{field}: {count}")
    #     print("\n")

if __name__ == "__main__":
    main()


