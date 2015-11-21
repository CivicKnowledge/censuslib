# Mixins for creating sources


from ambry.bundle import CaptureException

class MakeSourcesMixin(object):



    def test_mk_sources(self):

        for t in self.tables:
            self.make_source(t)
            print t.name

        self.commit()



