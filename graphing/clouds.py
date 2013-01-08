import numpy
import matplotlib.pyplot


class VMDistribution(object):

    def __init__(self, input_file, output_img_path):
        self.output_img_path = output_img_path
        self.input_file = input_file

    def parse_file(self):
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


#To use:
#x = VMDistribution("/Users/ali/Downloads/multi-cloud-worker-pool.log", "/Users/ali/ali.png")
#x.graph_data()

