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

            #print "we are this %d far from zero when we begin" % (seconds_when_jobs_start_running)

            # how far are we from zero when the jobs was terminated
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
        ax2.set_ylim(0, 45)
        ax.set_ylim(0,2100)


        # clouds

        data_dict = self.parse_input_file()
        desired_dict = self.get_desired_data()

        #start_time = self.get_start_time()
        start_time = min(data_dict['timestamp'])

        time_stamp_to_seconds = [ int(x - start_time) for x in data_dict['timestamp'] ]

        # Hotel: Running, Sierra: Running, Hotel: Desired, Sierra: Desired
        ax3 = fig.add_subplot(2,1,2)
        ax3.plot(time_stamp_to_seconds, data_dict['hotel_data'], label="Hotel: Running")
        ax3.plot(time_stamp_to_seconds, data_dict['sierra_data'], label="Sierra: Running")
        ax3.plot(time_stamp_to_seconds, [ desired_dict['hotel'] ] *len(time_stamp_to_seconds), '--', label="Hotel: Desired", color="#AAAAAA")
        ax3.plot(time_stamp_to_seconds, [ desired_dict['sierra'] ] *len(time_stamp_to_seconds), '-.', label="Sierra: Desired", color="#8D8D8D")

        # Put a legend below current axis
        box = ax3.get_position()
        ax3.set_position([box.x0, box.y0 + box.height * 0.1,
                 box.width, box.height * 0.9])
        ax3.legend(loc=8, bbox_to_anchor=(0.5, -0.4), ncol=4, prop={'size':10})

        ax3.set_xlabel("Second")
        ax3.set_ylabel("Number of Instances")
        ax3.set_ylim(0,45)
        ax3.set_xlim(ax.get_xlim())
        figure_path = os.path.join(self.input_dir,"condor_cloud.gif")
        matplotlib.pyplot.savefig(figure_path)


if __name__ == '__main__':
    grapher = CondorAndJobs(sys.argv[1])
    grapher.draw()

