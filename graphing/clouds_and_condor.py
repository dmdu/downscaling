import numpy
import matplotlib.pyplot
import datetime
import random
import os
import sys
import time

from lib.util import read_config
from parsing import log_parser

class CondorAndJobs(object):

    def __init__(self, input_dir):
        self.input_dir = input_dir
        self.config_data = read_config(os.path.join(self.input_dir,"workers.conf"))
        self.cpobj = log_parser.CondorParser(os.path.join(self.input_dir,"sleep.log"))
        self.cpobj.parse_file()
        self.condor_data = self.cpobj.condor_jobs_db

    def get_start_time(self):
        alist = []
        for ek, ev in self.condor_data.iteritems():
            alist.append(time.mktime(ev.submitted_time))
        return int(min(alist))

    def get_end_time(self):
        alist = []
        for ek, ev in self.condor_data.iteritems():
            print ek, ev
            alist.append(time.mktime(ev.terminated_time))
        return int(max(alist))

    def get_jobs_time(self,jobs_time):
        alist = []
        for ek, ev in self.condor_data.iteritems():
            if jobs_time == "scheduled" or jobs_time == "running":
                alist.append(time.mktime(ev.scheduled_time))
            elif jobs_time == "submitted":
                alist.append(time.mktime(ev.submitted_time))
            elif jobs_time == "terminated" or jobs_time == "completed":
                alist.append(time.mktime(ev.terminated_time))
        return alist


    def parse_input_file(self):
        data_base = {"timestamp":[], "hotel_data":[], "sierra_data":[]}
        with open(os.path.join(self.input_dir,"worker_pool.log")) as file_object:
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

    def get_desired_data(self):
        desired_dict = {"hotel":0, "sierra":0}
        desired_dict['hotel'] = self.config_data.getint("hotel","desired")
        desired_dict['sierra'] = self.config_data.getint("sierra","desired")
        return desired_dict


    def generate_submitted_time_data(self):
        end_time = self.get_end_time()
        start_time = self.get_start_time()
        total_seconds = int((datetime.datetime.fromtimestamp(end_time) - datetime.datetime.fromtimestamp(start_time)).total_seconds())
        x_values = range(total_seconds)
        jobs = [0] * len(x_values)

        for job_id, job_times in self.condor_data.iteritems():


        # how far are we from zero when the jobs was terminated
            seconds_when_jobs_submitted = int(time.mktime(job_times.submitted_time) - start_time )

            #print "we are this %d far from zero when we submitted" % (seconds_when_jobs_submitted)

            # the range we need to update
            range_to_alter = range(seconds_when_jobs_submitted, total_seconds)

            #print "the range to alter is %s" % str(range_to_alter)

            for index in range_to_alter:
                jobs[index] += 1

        return x_values, jobs


    def generate_completed_time_data(self):

        end_time = self.get_end_time()
        start_time = self.get_start_time()
        total_seconds = int((datetime.datetime.fromtimestamp(end_time) - datetime.datetime.fromtimestamp(start_time)).total_seconds())
        x_values = range(total_seconds)
        jobs = [0] * len(x_values)
        for job_id, job_times in self.condor_data.iteritems():

            # how far are we from zero when the jobs was terminated
            seconds_when_jobs_terminated = int(time.mktime(job_times.terminated_time) - start_time )

            #print "we are this %d far from zero when we terminated" % (seconds_when_jobs_terminated)

            # the range we need to update
            range_to_alter = range(seconds_when_jobs_terminated, total_seconds)

            #print "the range to alter is %s" % str(range_to_alter)

            for index in range_to_alter:
                jobs[index] += 1

        return x_values, jobs


    def generate_running_time_data(self):

        end_time = self.get_end_time()
        start_time = self.get_start_time()
        total_seconds = int((datetime.datetime.fromtimestamp(end_time) - datetime.datetime.fromtimestamp(start_time)).total_seconds())
        x_values = range(total_seconds)
        jobs = [0] * len(x_values)
        for job_id, job_times in self.condor_data.iteritems():


        # how far are we from zero when the job was started
            seconds_when_jobs_start_running = int(time.mktime(job_times.scheduled_time) - start_time)
            # how far are we from zero when the jobs was terminated
            if job_times.evicted_time:
                seconds_when_jobs_terminated = int(time.mktime(job_times.evicted_time) - start_time )
                range_to_alter = range(seconds_when_jobs_start_running, seconds_when_jobs_terminated)
                for index in range_to_alter:
                    jobs[index] += 1

                seconds_when_jobs_start_running = int(time.mktime(job_times.rescheduled_time) - start_time)
                seconds_when_jobs_terminated = int(time.mktime(job_times.terminated_time)) - start_time
                range_to_alter = range(seconds_when_jobs_start_running, seconds_when_jobs_terminated)
                for index in range_to_alter:
                    jobs[index] += 1

                continue

            seconds_when_jobs_terminated = int(time.mktime(job_times.terminated_time) - start_time )

            #print "we are this %d far from zero when we terminated" % (seconds_when_jobs_terminated)

            # the range we need to update
            range_to_alter = range(seconds_when_jobs_start_running, seconds_when_jobs_terminated)

            #print "the range to alter is %s" % str(range_to_alter)

            for index in range_to_alter:
                jobs[index] += 1

        return x_values, jobs





    def draw(self):

        # condor :

        fig = matplotlib.pyplot.figure()
        ax = fig.add_subplot(2,1,1)

        cx, cy = self.generate_completed_time_data()
        lns1 = ax.plot(cx,cy, '-g', label="completed")

        sx, sy = self.generate_submitted_time_data()
        lns2 = ax.plot(sx,sy, '-b', label="submitted")

        rx, ry = self.generate_running_time_data()
        ax2 = ax.twinx()
        lns3 = ax2.plot(rx, ry, '-r', label = 'running')

        # to hide the first zeros in x and y axis
        frame1 = matplotlib.pyplot.gca()
        frame1.axes.get_xticklabels()[0].set_visible(False)
        frame1.axes.get_yticklabels()[0].set_visible(False)

        # to group the legend of job running and completed/submitted together
        lns = lns1+lns2+lns3
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs, loc=6, prop={'size':10})

        # set labels and limit
        ax.set_xlabel("Second")
        ax.set_ylabel("Jobs Submitted / Complete")
        ax2.set_ylabel("Jobs Running")
        ax2.set_ylim(0, 70)
        ax.set_ylim(0,1150)
        ax.set_xlim(0,12000.0)
        ax2.set_xlim(0,12000.0)


        # clouds

        data_dict = self.parse_input_file()
        desired_dict = self.get_desired_data()

        #start_time = self.get_start_time()
        #end_time = self.get_end_time()

        start_time = min(data_dict['timestamp'])
        end_time = max(data_dict['timestamp'])

        #time_stamp_to_seconds = [ int(x - start_time) for x in data_dict['timestamp'] ]


        total_seconds = int((datetime.datetime.fromtimestamp(end_time) - datetime.datetime.fromtimestamp(start_time)).total_seconds())


        x_values = range(total_seconds)
        instances_h = [0] * len(x_values)
        instances_s = [0] * len(x_values)
        tslist = data_dict['timestamp']

        for instance_index, instance_time in enumerate(data_dict['timestamp']):

            # how far are we from zero when the job was started
            seconds_when_jobs_start_running = int(instance_time - start_time)

            # the range we need to update
            if instance_index == 0:
                range_to_alter = range(0, seconds_when_jobs_start_running)
            else:
                tmp_value = int(tslist[instance_index -1] - start_time)
                range_to_alter = range( tmp_value , seconds_when_jobs_start_running)


            for index in range_to_alter:
                instances_h[index] = data_dict['hotel_data'][instance_index -1]
                instances_s[index] = data_dict['sierra_data'][instance_index -1]


    # Hotel: Running, Sierra: Running, Hotel: Desired, Sierra: Desired
        ax3 = fig.add_subplot(2,1,2)
        ax3.plot(x_values, instances_h, label="Hotel: Running")
        ax3.plot(x_values, instances_s, label="Sierra: Running")
        ax3.plot(x_values, [ desired_dict['hotel'] ]  * len(instances_h), '--', label="Hotel: Desired",linewidth=5.0, color="#AAAAAA")
        ax3.plot(x_values, [ desired_dict['sierra'] ] * len(instances_h), '-.', linewidth=5.0, label="Sierra: Desired", color="#8D8D8D")

        # Put a legend below current axis
        box = ax3.get_position()
        ax3.set_position([box.x0, box.y0 + box.height * 0.1,
                 box.width, box.height * 0.9])
        ax3.legend(loc=8, bbox_to_anchor=(0.5, -0.4), ncol=4, prop={'size':10})

        ax3.set_xlabel("Second")
        ax3.set_ylabel("Number of Instances")
        ax3.set_ylim(0,70)
        ax3.set_xlim(ax.get_xlim())
        figure_path = os.path.join(self.input_dir,"condor_cloud.gif")
        matplotlib.pyplot.savefig(figure_path)
        #matplotlib.pyplot.savefig("/tmp/ss.gif")


    def writeout_useful_info(self):

        tmp_info_list = []
        data_to_graph = self.parse_input_file()
        timestamp_to_datetime = [ datetime.datetime.fromtimestamp(x) for x in data_to_graph["timestamp"] ]
        data_to_graph["timestamp"] = timestamp_to_datetime

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


if __name__ == '__main__':
    grapher = CondorAndJobs(sys.argv[1])
    grapher.draw()
    to_write = grapher.writeout_useful_info()
    summary_file_name = os.path.join(sys.argv[1],"summary.txt")
    with open(summary_file_name, 'w') as myFileObj:
         lines = grapher.writeout_useful_info()
         for line in lines:
             myFileObj.write("%s\n" % line)
