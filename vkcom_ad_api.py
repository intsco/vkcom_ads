from __future__ import division
import tornado.ioloop
import tornado.web
import os.path
from vkcom_click_validator import ClickValidator


base_path = os.path.dirname(os.path.realpath(__file__)) + '/data/'


class ReportHandler(tornado.web.RequestHandler):
    def get(self):
        report_fn = base_path + 'report.csv'

        if validator_state.state == 'DONE':
            self.write(open(report_fn).read())
        elif validator_state.state == 'UNKNOWN':
            self.write('No validation report yet')
        else:
            self.write('Validation is in progress...')


class ValidateHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        # tornado.ioloop.IOLoop.instance().spawn_callback(validator.validate)
        validator_state.state = 'IN PROGRESS'
        ClickValidator(self.validator_done).start()
        self.finish()

    def validator_done(self, value):
        validator_state.state = value


class UploadHandler(tornado.web.RequestHandler):
    def post(self):
        report_fn = base_path + 'report.csv'
        data_fn = base_path + 'click_data.gz'
        data = self.request.files['filedata'][0]['body']

        open(data_fn, 'w').writelines(data)

        if os.path.isfile(report_fn):
            os.remove(report_fn)


class ValidatorState():
    def __init__(self):
        self.state = 'UNKNOWN'


if __name__ == "__main__":
    validator_state = ValidatorState()

    application = tornado.web.Application([
        (r"/report/?", ReportHandler),
        (r"/validate/?", ValidateHandler),
        (r"/upload/?", UploadHandler),
    ])
    
    application.listen(8899)
    tornado.ioloop.IOLoop.instance().start()