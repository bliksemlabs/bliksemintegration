from osgeo import osr, ogr
 
src_string = '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +towgs84=565.2369,50.0087,465.658,-0.406857330322398,0.350732676542563,-1.8703473836068,4.0812 +no_defs no_defs <>'
dst_string = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
  
src = osr.SpatialReference()
src.ImportFromProj4(src_string)
   
dst = osr.SpatialReference()
dst.ImportFromProj4(dst_string)
    
def rdwgs84(x, y):
    ogr_geom = ogr.CreateGeometryFromWkt('POINT(%d %d)' % (x, y))
    ogr_geom.AssignSpatialReference(src)
    ogr_geom.TransformTo(dst)
      
    return (round(ogr_geom.GetX(), 6), round(ogr_geom.GetY(), 6))
