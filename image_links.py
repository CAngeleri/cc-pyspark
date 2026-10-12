from urllib.parse import urljoin

from pyspark.sql.types import StructType, StructField, StringType

from wat_extract_links import ExtractLinksJob


class ImageLinksJob(ExtractLinksJob):
    """Extract links from WAT files with their link type (IMG@/src, og:image, ...)"""

    name = "ImageLinks"

    output_schema = StructType([
        StructField("s", StringType(), True),
        StructField("t", StringType(), True),
        StructField("type", StringType(), True)
    ])

    def yield_link(self, src, target):
        yield src, target, "redirect"

    def yield_links(self, src_url, base_url, links, url_attr, opt_attr=None):
        if not base_url:
            base_url = src_url
        has_links = False
        for l in links:
            link_type = l.get('path') or l.get('property') or l.get('name')
            if url_attr in l:
                link = l[url_attr]
            elif opt_attr in l and ExtractLinksJob.url_abs_pattern.match(l[opt_attr]):
                link = l[opt_attr]
            else:
                continue
            try:
                lurl = urljoin(base_url, link)
            except ValueError:
                continue
            has_links = True
            yield src_url, lurl, link_type
        if not has_links:
            yield src_url, src_url, "self"

    def yield_http_header_links(self, url, headers):
        for l in self.extract_http_header_links(url, headers):
            yield url, l, "http-header"

if __name__ == "__main__":
    job = ImageLinksJob()
    job.run()
