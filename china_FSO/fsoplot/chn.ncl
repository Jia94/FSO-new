procedure attach_china_map(wks, plot)
local res, res_scs, res_small_map, map_scs, shapefile_root

begin
  shapefile_root="./china"
  res = True
  res@gsLineThicknessF = 0.6
  res@gsLineOpacityF   = 0.5
;  plot@simplied_china_map = gsn_add_shapefile_polylines(wks, plot, shapefile_root + "/simplied_china_country.shp", res)
  res@gsLineThicknessF = 0.5
;  plot@china_map = gsn_add_shapefile_polylines(wks, plot, shapefile_root + "/china.shp", res)
  res@gsLineThicknessF = 1.5
  plot@china_nine_dotted_line_map = gsn_add_shapefile_polylines(wks, plot, shapefile_root + "/china_nine_dotted_line.shp", res)

  res_scs = True
  res_scs@gsnDraw = False
  res_scs@gsnFrame = False
  res_scs@vpWidthF = 0.15
  res_scs@mpMinLonF = 105.0
  res_scs@mpMaxLonF = 123.0
  res_scs@mpMinLatF = 2.0
  res_scs@mpMaxLatF = 23.5
  res_scs@mpOceanFillColor = 0
  res_scs@tmXBOn = False
  res_scs@tmXTOn = False
  res_scs@tmYLOn = False
  res_scs@tmYROn = False
  fnscs = addfile("./ter2.nc","r")
  hgt=fnscs->btdata(:,:)
  hgt = hgt
  res_scs@cnLevelSelectionMode = "ExplicitLevels"   ; set explicit contour levels
  res_scs@cnLevels    = (/ 0,100,200,500,1000,2000,3000,4000,5000/)   ; set levels
;  cmap      = read_colormap_file("OceanLakeLandSnow");topo_15lev");MPL_gist_earth");MPL_terrain");OceanLakeLandSnow")
  cmap      = read_colormap_file("OceanLakeLandSnow")
  cmap(:,3) = 0.5      
  res_scs@cnFillOn              = True            ; color plot desired
  res_scs@lbLabelsOn            = False
  res_scs@cnLineLabelsOn        = False
  res_scs@cnFillPalette         = cmap
  res_scs@cnLinesOn             = False           ; turn off contour lines
  res_scs@lbLabelBarOn   = False
  res_scs@tiMainString          = ""
  res_scs@gsnLeftString         =""
  res_scs@gsnRightString        =""
  res_scs@gsnCenterString       =""
  res@gsLineThicknessF = 0.5
  res@gsLineOpacityF   = 0.3
  res_scs@mpFillOn         = False
  map_scs = gsn_csm_contour_map(wks,hgt,res_scs)
  plot@china_map_in_small_map = gsn_add_shapefile_polylines(wks, map_scs, shapefile_root + "/china.shp", res)
  res@gsLineOpacityF   = 0.5
  res@gsLineThicknessF   = 2.0
  plot@china_nine_dotted_line_in_small_map = gsn_add_shapefile_polylines(wks, map_scs, shapefile_root + "/china_nine_dotted_line.shp", res)

  res_small_map = True
  res_small_map@amParallelPosF = -0.495
  res_small_map@amOrthogonalPosF = 0.495
  res_small_map@amJust = "BottomLeft"
  plot@south_china_sea_small_map = gsn_add_annotation(plot, map_scs, res_small_map)
end


undef ("add_Changjiang_Huanghe")
procedure add_Changjiang_Huanghe(wks[1]:graphic,plot[1]:graphic,opt[1]:logical)
local resp,wks,opt,nhh,ncj,dumh,dumc 
begin
  xptcj=(/121.383  ,121.267  ,121.100  , \
          120.967  ,120.850  ,120.700  ,120.567  ,120.500  ,120.333  , \
          120.167  ,120.050  ,119.917  ,119.900  ,119.826  ,118.971  , \
          118.535  ,117.984  ,117.921  ,117.184  ,116.736  ,116.164  , \
          115.638  ,115.021  ,114.650  ,114.065  ,114.275  ,113.876  , \
          113.262  ,113.116  ,113.232  ,112.811  ,112.783  ,112.583  , \
          112.635  ,112.418  ,112.426  ,112.066  ,111.763  ,111.348  , \
          110.162  ,108.836  ,107.930  ,106.797  ,106.352  ,106.085  , \
          106.044  ,105.802  ,105.313  ,105.196  ,104.670  ,104.059  , \
          104.087  ,103.660  ,103.711  ,103.094  ,103.192  ,102.887  , \
          102.776  ,102.076  ,102.153  ,102.010  ,101.720  ,101.667  , \
          101.150  ,100.660  ,100.602  ,100.751  ,100.613  ,100.177  , \
           99.860  , 99.367  , 99.164  , 98.818  , 99.089  , 98.422  , \
           97.676  , 97.226  , 96.136  , 95.545  , 94.759  , 94.264  , \
           94.018  , 93.033  , 91.430  , 90.778  /)

  yptcj=(/ 31.613  , 31.675  , 31.800  , \
           31.817  , 31.963  , 32.033  , 32.100  , 32.050  , 32.000  , \
           31.987  , 32.000  , 32.100  , 32.188  , 32.249  , 32.211  , \
           31.374  , 31.183  , 30.878  , 30.534  , 29.964  , 29.801  , \
           29.939  , 30.672  , 30.735  , 30.278  , 30.152  , 30.017  , \
           29.506  , 29.653  , 29.839  , 29.780  , 29.907  , 29.792  , \
           30.015  , 30.142  , 30.368  , 30.489  , 30.333  , 30.908  , \
           31.133  , 31.012  , 29.942  , 29.676  , 29.159  , 29.111  , \
           28.879  , 29.033  , 28.777  , 28.901  , 28.703  , 28.728  , \
           28.384  , 28.185  , 27.900  , 27.340  , 26.418  , 26.284  , \
           26.440  , 26.036  , 26.409  , 26.668  , 26.656  , 26.480  , \
           26.263  , 26.279  , 26.853  , 27.503  , 27.908  , 26.948  , \
           27.388  , 28.497  , 30.810  , 31.293  , 31.420  , 32.408  , \
           32.737  , 33.381  , 33.956  , 34.550  , 34.822  , 34.616  , \
           34.322  , 34.143  , 34.391  , 34.333  /)

  xpthh=(/ 96.192  , 97.611  , 97.989  , 98.195  , 98.597  , 98.534  , \
           98.714  , 99.263  , 99.278  , 99.520  , 99.967  ,100.367  , \
          100.457  ,100.993  ,101.209  ,101.242  ,101.598  ,101.972  , \
          102.234  ,102.570  ,102.690  ,102.340  ,101.440  ,101.069  , \
          100.546  ,100.343  ,100.513  ,101.067  ,101.554  ,101.958  , \
          102.314  ,103.339  ,103.655  ,104.237  ,104.402  ,104.923  , \
          104.801  ,104.537  ,104.521  ,105.205  ,106.105  ,107.081  , \
          106.919  ,107.477  ,107.962  ,108.639  ,108.982  ,109.412  , \
          110.415  ,111.584  ,111.637  ,111.317  ,111.445  ,111.189  , \
          111.103  ,110.715  ,111.002  ,110.883  ,110.577  ,110.825  , \
          110.476  ,110.455  ,110.621  ,111.772  ,112.152  ,113.002  , \
          113.630  ,114.066  ,114.934  ,115.272  ,116.249  ,117.442  , \
          118.427  ,118.537  ,119.076  /)

  ypthh=(/ 35.183  , 34.880  , 35.169  , 35.151  , 34.875  , 34.642  , \
           34.470  , 34.469  , 34.032  , 33.801  , 33.869  , 33.635  , \
           33.822  , 33.988  , 33.790  , 33.946  , 33.737  , 33.743  , \
           33.426  , 33.464  , 33.601  , 34.027  , 34.364  , 34.781  , \
           34.900  , 35.531  , 35.823  , 36.177  , 36.085  , 36.215  , \
           35.875  , 35.818  , 36.228  , 36.119  , 36.388  , 36.643  , \
           36.886  , 36.951  , 37.215  , 37.521  , 37.658  , 39.092  , \
           40.111  , 40.698  , 40.919  , 40.881  , 40.620  , 40.548  , \
           40.599  , 40.143  , 39.585  , 39.446  , 39.365  , 39.048  , \
           38.586  , 38.247  , 37.720  , 37.348  , 37.070  , 35.677  , \
           35.023  , 34.722  , 34.648  , 34.925  , 35.159  , 34.854  , \
           35.067  , 34.935  , 34.975  , 35.443  , 36.033  , 37.091  , \
           37.441  , 37.655  , 37.876  /)

  nhh = dimsizes(xpthh)
  ncj = dimsizes(xptcj)

;
; Draw Yangtze River and Yellow River in China 
;
  resp                   = True                     ; polyline mods desired
  resp@gsLineLabelString = ""                       ; adds a line label string

  if (opt .and. isatt(opt,"color")) then
     resp@gsLineColor = opt@color 
  else
     resp@gsLineColor = "black"   
  end if
  if (opt .and. isatt(opt,"thickness")) then
     resp@gsLineThicknessF = opt@thickness 
  else
     resp@gsLineThicknessF = 2              ; twice as thick
  end if
  if (opt .and. isatt(opt,"dashP")) then
     resp@gsLineDashPattern = opt@dashP 
  else
     resp@gsLineDashPattern = 0           ; solid line
  end if
  resp@gsLineOpacityF   = 0.3
  plot@dumhP=gsn_add_polyline(wks,plot,xpthh,ypthh,resp)
  plot@dumcP=gsn_add_polyline(wks,plot,xptcj,yptcj,resp)
end
