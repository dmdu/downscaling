import numpy
import matplotlib.pyplot
import datetime
import os
import sys
from lib.util import read_config


class VMDistribution(object):

    def __init__(self, input_file, output_img_path, config_file, failure_file):
        self.output_img_path = output_img_path
        self.input_file = input_file
        self.failure_file = failure_file
        self.config_file = config_file


    def get_desired_data(self):
        desired_dict = {"hotel":0, "sierra":0}
        config_data = read_config(self.config_file)
        desired_dict['hotel'] = config_data.getint("hotel","desired")
        desired_dict['sierra'] = config_data.getint("sierra","desired")
        return desired_dict


    def parse_input_file(self):
        data_base = {"timestamp":[], "hotel_data":[], "sierra_data":[]}
        with open(self.input_file) as file_object:
            for line in file_object:
                if line :
                    line_as_array = line.strip().split()
                    data_as_array = line_as_array[-1].split(",")
                    hotel_data = int(data_as_array[2].split(":")[1])
                    sierra_data = int(data_as_array[1].split(":")[1])
                    timestamp = float(data_as_array[0])
                    data_base["hotel_data"].append(hotel_data)
                    data_base["sierra_data"].append(sierra_data)
                    data_base["timestamp"].append(timestamp)
        return data_base

    def parse_failure_file(self):
        data_base = {"timestamp":[]}
        with open(self.failure_file) as file_object:
            for line in file_object:
                if line :
                    line_as_array = line.strip().split()
                    data_as_array = line_as_array[2].split(",")
                    timestamp = float(data_as_array[0])
                    data_base['timestamp'].append(timestamp)
        return data_base

    def graph_data_with_date_and_failure(self):

        # get date, convert them to datetime
        data_to_graph = self.parse_input_file()
        timestamp_to_datetime = [ datetime.datetime.fromtimestamp(x) for x in data_to_graph["timestamp"] ]
        data_to_graph["timestamp"] = timestamp_to_datetime

        # increse the ylimit
        ylimit = max( max(data_to_graph['hotel_data']), max(data_to_graph['sierra_data']) ) + 2
        matplotlib.pyplot.ylim(0, ylimit)

        # get failure date and plot it
        failure_data = self.parse_failure_file()
        timestamp_to_datetime_failure = [ datetime.datetime.fromtimestamp(x) for x in failure_data["timestamp"] ]
        failure_data['timestamp'] = timestamp_to_datetime_failure
        matplotlib.pyplot.vlines(failure_data["timestamp"],0,1,label="failure detected")

        # desired data
        desired_dict = self.get_desired_data()

        # draw hotel, sierra and desired ratio
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=data_to_graph['hotel_data'], fmt="r-o", label='Hotel')
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=data_to_graph['sierra_data'], fmt="b-o", label='Sierra')
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=[ desired_dict['hotel'] ] *len(data_to_graph['timestamp']), linewidth=3.0, fmt="--", label="Desired: Hotel", color="#AAAAAA")
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=[ desired_dict['sierra'] ] *len(data_to_graph['timestamp']), linewidth=3.0, fmt="-.", label="Desired: Sierra", color="#8D8D8D")

        # draw the figure, legend and safe it
        matplotlib.pyplot.title("VM Distribution Across Clouds Over Time")
        matplotlib.pyplot.ylabel("Instance count")
        matplotlib.pyplot.xlabel("Time")
        matplotlib.pyplot.legend(loc=0)
        matplotlib.pyplot.savefig(self.output_img_path)

    def graph_data_with_date(self):
        # get date, convert them to datetime
        data_to_graph = self.parse_input_file()
        timestamp_to_datetime = [ datetime.datetime.fromtimestamp(x) for x in data_to_graph["timestamp"] ]
        data_to_graph["timestamp"] = timestamp_to_datetime

        # increse the ylimit
        ylimit = max( max(data_to_graph['hotel_data']), max(data_to_graph['sierra_data']) ) + 2
        matplotlib.pyplot.ylim(0, ylimit)

        # desired data
        desired_dict = self.get_desired_data()

        # draw hotel, sierra and desired ratio
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=data_to_graph['hotel_data'], fmt="r-o", label='Hotel')
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=data_to_graph['sierra_data'], fmt="b-o", label='Sierra')
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=[ desired_dict['hotel'] ] *len(data_to_graph['timestamp']), linewidth=3.0, fmt="--", label="Desired: Hotel", color="#AAAAAA")
        matplotlib.pyplot.plot_date(x=data_to_graph['timestamp'], y=[ desired_dict['sierra'] ] *len(data_to_graph['timestamp']), linewidth=3.0, fmt="-.", label="Desired: Sierra", color="#8D8D8D")

        # draw the figure, legend and safe it
        matplotlib.pyplot.title("VM Distribution Across Clouds Over Time")
        matplotlib.pyplot.ylabel("Instance count")
        matplotlib.pyplot.xlabel("Time")
        matplotlib.pyplot.legend(loc=0)
        matplotlib.pyplot.savefig(self.output_img_path)


    def writeout_useful_info(self):

        tmp_info_list = []
        data_to_graph = self.parse_input_file()
        timestamp_to_datetime = [ datetime.datetime.fromtimestamp(x) for x in data_to_graph["timestamp"] ]
        data_to_graph["timestamp"] = timestamp_to_datetime

        # get total time of execution
        total_time_of_exec = data_to_graph["timestamp"][-1] - data_to_graph["timestamp"][0]
        tmp_info_list.append("The total time of execution is %s" % (str(total_time_of_exec)))

        # desired data
        desired_dict = self.get_desired_data()


        # get first time of convergence
        hotel_desired = desired_dict['hotel']
        sierra_desire = desired_dict['sierra']
        total_in_conv_state = 0
        conv_time = False
        for index, data in enumerate(data_to_graph["timestamp"]):
            if data_to_graph['hotel_data'][index] == hotel_desired and data_to_graph['sierra_data'][index] == sierra_desire:
                if not conv_time:
                    conv_time = data
                total_in_conv_state +=1

        if conv_time:
            tmp_info_list.append("First convergence time is %s" % (conv_time))
            tmp_info_list.append("First convergence time happened after %s" % (conv_time - data_to_graph["timestamp"][0]))
        else:
            tmp_info_list.append("no convergence time")

        # get percentage of convergence
        percentage = (float(total_in_conv_state)/len(data_to_graph["timestamp"])) * 100.0
        tmp_info_list.append("Percentage of time in the desired state %f" % (percentage))

        return tmp_info_list

    def graph_data(self):
        data_to_graph = self.parse_file()
        indexs = numpy.arange(len(data_to_graph["timestamp"]))
        rect_width = 0.35
        figure = matplotlib.pyplot.figure()
        hotel_rect = matplotlib.pyplot.bar(indexs, data_to_graph["hotel_data"], rect_width, color='r')
        sierra_rect = matplotlib.pyplot.bar(indexs + rect_width, data_to_graph["sierra_data"], rect_width, color='y')
        figure.autofmt_xdate(rotation=25)
        matplotlib.pyplot.ylabel('VMS Count')
        matplotlib.pyplot.title("VMS Distribution")
        matplotlib.pyplot.legend( (hotel_rect[0], sierra_rect[0]), ('Hotel', 'Sierra') )
        half_ticks = len(data_to_graph["timestamp"])/2
        matplotlib.pyplot.xticks(2 * (indexs[:half_ticks] + rect_width), data_to_graph["timestamp"][:half_ticks])
        matplotlib.pyplot.savefig(self.output_img_path)


def run(dir_abs_path):

    graph_file_name = os.path.join(dir_abs_path, "%s.gif" % (os.path.basename(dir_abs_path)))
    summary_file_name = os.path.join(dir_abs_path, "%s-summary.txt" % (os.path.basename(dir_abs_path)))
    worker_pool_log = os.path.join(dir_abs_path, "worker_pool.log")
    failure_file = os.path.join(dir_abs_path,"failure.log")
    workers_config = os.path.join(dir_abs_path,"workers.conf")

    if os.path.isfile(failure_file):
        graph_obj = VMDistribution(worker_pool_log, graph_file_name, workers_config, failure_file)
        graph_obj.graph_data_with_date_and_failure()
    else:
        graph_obj = VMDistribution(worker_pool_log, graph_file_name, workers_config, None)
        graph_obj.graph_data_with_date()


    with open(summary_file_name, 'w') as myFileObj:
        lines = graph_obj.writeout_useful_info()
        for line in lines:
            myFileObj.write("%s\n" % line)

    print "done processing %s" % (dir_abs_path)


if __name__ == '__main__':
    run(sys.argv[1])
    #run(None)

