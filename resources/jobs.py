import logging

LOG = logging.getLogger(__name__)

class Job(object):

    def __init__(self, id, running, node):

        self.id = id
        self.running = running
        self.node = node

class Jobs(object):

    def __init__(self, queue_state, user):

        self.list = list()
        if queue_state != None:
            items = queue_state.split()
            if user in items:
                start = items.index(user) - 1
                for i in range(start, len(items), 6):
                    print "Job %s running for %s on %s" % (items[i], items[i+4], items[i+5])
                    self.list.append(Job(items[i], items[i+4], items[i+5]))