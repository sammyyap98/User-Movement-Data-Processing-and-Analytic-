import csv
import glob
import os
import pandas as pd
import random

#####
def run(input_size, target_size, combine_csv, addition_randomized_dataset):

    input_size = input_size
    target_size = target_size
    total_data_count = 0
    result_file_count = 0
    result_file_name = '.csv'

    raw_data_path = '.\\Data\\csv_files(raw_data)\\new\\*.csv'
    processed_data_path = '.\\Data\\processed_csv_file_msp\\'

    class RawDataFormat:
        def __init__(self, date, time, user, source, destination):
            self.date = date
            self.time = time
            self.user = user
            self.source = source
            self.destination = destination

        def __cmp__(self, other):
            if self.user == other.user and self.source == other.source and self.destination == other.destination:
                return 1
            else:
                return 0


    log_cnt = 0


    def raw_type_converter(file_path):
        # Read CSV file
        file = open(file_path, 'r', encoding='utf-8')
        csv_reader = csv.reader(file)
        #global log_cnt
        log_cnt = 0
        processing_list = []
        for line in csv_reader:
            log_cnt += 1
            temp = str(line)
            if temp.__contains__('%AUTHMGR-4-UNAUTH_MOVE'):
                temp = temp.replace('[', '')
                temp = temp.replace(']', '')
                temp = temp.replace('\'', '')
                temp = temp.split(',')
                # date,time
                date_time = temp[0].split(' ')
                # user
                temp[3] = temp[3].replace('(slow)', '')
                temp[3] = temp[3].replace('(fast)', '')
                user = temp[3][temp[3].find('(') + 1:temp[3].find(')')]
                # source, destination
                source_destination = temp[3].split('from')
                source_destination = source_destination[1].replace(
                    ' ', '').split('to')
                source = source_destination[0].replace('Ca', '')
                destination = source_destination[1].replace('Ca', '')
                # data list collection
                temp_raw_data = RawDataFormat(
                    date_time[0], date_time[1], user, source, destination)
                processing_list.append(temp_raw_data)
                # duplicated data delete, only straight sequential data
                if not len(processing_list) == 1:
                    if processing_list[-2].__cmp__(temp_raw_data):
                        processing_list.pop()
            else:
                continue
        file.close()
        return reversed(processing_list)



    def connection_sequence_geneator(roaming_data):
        connection_map = {}
        for data_ in roaming_data:
            # history sequence find
            if connection_map.__contains__(data_.user):
                # check sequences can be made
                if connection_map[data_.user][-1][-1] == data_.source:
                    connection_map[data_.user][-1] += [data_.destination]
                else:
                    connection_map[data_.user].append(
                        [data_.source, data_.destination])
            else:
                connection_map[data_.user] = [[data_.source, data_.destination]]

        # print(connection_map)
        return connection_map


    ap_number_onehot_mapping_list = [[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
                                [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
                                [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
                                [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
                                [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
                                [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
                                [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
                                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
                                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                                '\n', '\t']


    def convert_ap_number_to_char(sequence_path_list):
        temp = []
        for ap in sequence_path_list:
            temp.append(ap_number_onehot_mapping_list[int(ap)])

            # print(ap)
            # print(ap_number_onehot_mapping_list[int(ap)])
        return temp


    def training_data_formater(result_file, sequenced_path_list, input_size, target_size):
        # Write CSV file
        file = open(result_file, 'w', newline='', encoding='utf-8')
        csv_writer = csv.writer(file)

        for list_name in sequenced_path_list:
            # print(list)
            for path in sequenced_path_list[list_name]:
                converted_path = convert_ap_number_to_char(path)

                if len(converted_path) >= input_size + target_size:
                    # print(path)
                    for i in range(0, len(converted_path) - input_size - target_size):
                        # print(i)
                        # print(path[i:i + window_size+1])
                        csv_writer.writerow(
                            converted_path[i:i + input_size + target_size])

        file.close()




    path_2019 = processed_data_path + "2019\\" + \
        str(target_size) + "sequence"
    path_2020 = processed_data_path +"2020\\" + \
        str(target_size) + "sequence"

    if not os.path.isdir(path_2019):
        os.mkdir(path_2019)
        path_2019 = path_2019 + "/" + str(input_size) + "sequence"
        if not os.path.isdir(path_2019):
            os.mkdir(path_2019)
    else:
        path_2019 = path_2019 + "/" + str(input_size) + "sequence"
        if not os.path.isdir(path_2019):
            os.mkdir(path_2019)

    if not os.path.isdir(path_2020):
        os.mkdir(path_2020)
        path_2020 = path_2020 + "/" + str(input_size) + "sequence"
        if not os.path.isdir(path_2020):
            os.mkdir(path_2020)
    else:
        path_2020 = path_2020 + "/" + str(input_size) + "sequence"
        if not os.path.isdir(path_2020):
            os.mkdir(path_2020)

    for csv_file in glob.glob(raw_data_path):
        processed_list = raw_type_converter(csv_file)
        sequenced_connection_path_data = connection_sequence_geneator(
            processed_list)
        # print(sequenced_connection_path_data)
        result_file_count += 1
        # print('./processed_csv_file_msp/' + str(input_size) + 'sequence/' + str(result_file_count) + result_file_name)
        training_data_formater(path_2019 + "/" + str(result_file_count) +
                            result_file_name, sequenced_connection_path_data, input_size, target_size)

        #print(log_cnt)
    # Additional Collection
    for csv_file in glob.glob(raw_data_path):
        processed_list = raw_type_converter(csv_file)
        sequenced_connection_path_data = connection_sequence_geneator(
            processed_list)
        # print(sequenced_connection_path_data)
        result_file_count += 1
        # print('./processed_csv_file_msp/' + str(window_size) + 'sequence/' + str(result_file_count) + result_file_name)
        training_data_formater(path_2020 + "/" + str(result_file_count) +
                            result_file_name, sequenced_connection_path_data, input_size, target_size)
    print('file generation complete: ', input_size, '-', target_size)

 
    sequence_file_path = processed_data_path + '2019\\'+ str(target_size) +'sequence\\' + str(input_size) + 'sequence\\*.csv'

    # All csv combine into one file
    if combine_csv:
        total_dataset = []
        for csv_file in glob.glob(sequence_file_path):
            file = open(csv_file, 'r', encoding='utf-8')
            csv_reader = csv.reader(file)
            for line in csv_reader:
                # line = list(map(int, line))
                total_dataset.append(line[:])

        #print(total_dataset)
        df = pd.DataFrame(total_dataset)
        df.to_csv(processed_data_path + str(input_size) + '_' + str(target_size) + '.csv', index=False, header=False)

    # Additional randomized dataset
    if addition_randomized_dataset:
        random.shuffle(total_dataset)
        #print(total_dataset)
        df2 = pd.DataFrame(total_dataset)
        df2.to_csv(processed_data_path + str(input_size) + '_' + str(target_size) + '_randomize.csv', index=False, header=False)


for in_size in ([22]): #INPUT SEQUENCE SIZE
    for out_size in ([3,5,7,9]): #OUTPUT SEQUENCE SIZE
        run(in_size, out_size, combine_csv=False, addition_randomized_dataset = False)
