import nimrod
a = nimrod.Nimrod(open(
    'metoffice-c-band-rain-radar_uk_201601010000_1km-composite.dat.gz'))
a.query()
a.extract_asc(open('full_raster.asc', 'w'))
a.apply_bbox(279906, 285444, 283130, 290440)
a.query()
a.extract_asc(open('clipped_raster.asc', 'w'))