import os
import json
import diamond

class OpenstackSwiftReconCollector(diamond.collector.Collector):
    """
    Reads swift recon cache files to collect swift recon data
    """

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        return {'path': 'swiftrecon',
                'recon_account_cache':  '/var/cache/swift/account.recon',
                'recon_container_cache':  '/var/cache/swift/account.recon',
                'recon_object_cache':  '/var/cache/swift/object.recon',
                'method':   'Threaded'}

    def _process_cache(self, d, path=()):

        for k, v in d.iteritems():
            if not isinstance(v, dict):
                self.metrics.append((path + (k,), v))
            else:
                self._process_cache(v, path + (k,))

    def collect(self):

        self.metrics = []
        recon_cache = {'account': self.config['recon_account_cache'],
                       'container': self.config['recon_container_cache'],
                       'object': self.config['recon_object_cache']}

        for recon_type in recon_cache:
            if not os.access(recon_cache[recon_type], os.R_OK):
                continue
            with open(recon_cache[recon_type]) as f:
                try:
                    rmetrics = json.loads(f.readlines()[0].strip())
                    self.metrics = []
                    self._process_cache(rmetrics)
                    for k, v in self.metrics:
                        metric_name = '%s.%s' % (recon_type, ".".join(k))
                        if isinstance(v, (int, float)):
                            self.publish(metric_name, v)
                except (ValueError, IndexError):
                    continue
