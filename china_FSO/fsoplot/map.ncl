;*************************************************
;
; These files are loaded by default in NCL V6.2.0 and newer
 load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
 load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
 load "./varmap.ncl"
 load "./chn.ncl"
;----------------------------------------------------------------------
; Main code.
;----------------------------------------------------------------------
begin
;-------Options--------
   pr=abs(R)
   if (avg(pr).gt.5)then
     fanwei=floattoint(2.*(floattoint(avg(pr))-floattoint(avg(pr))%5))
   else
     fanwei=2
   end if
  fanwei=fanwei/5
  if (fanwei.gt.10)then
     fanwei=abs(fanwei-fanwei%10)
  end if
  if (fanwei.lt.2) then
     fanwei=2
  end if
;  print(fanwei)
  levels=ispan(-5*fanwei,5*fanwei,fanwei)
  nlevels = dimsizes(levels);rgba testcmap
  colors  = span_color_rgba("seaice_2",nlevels+1)
  ;gsn_define_colormap(wks,"b2c12") cmp_b2r GMT_panoply
  ;MPL_rainbow  BlueDarkRed18  testcmap
;---------------------------
  npts = dimsizes(lat)                                  ; Number of points.
 ; lat  = random_uniform( 10., 60.,npts)         ; Create some dummy latitude
 ; lon  = random_uniform(60.,150.,npts)         ; and longitude data that
 ; R   = random_uniform(-10,10.,npts)   ; This is dummy data for determining
                                        ; how to color the markers.

;------------------------------ 
; Create X and Y arrays to hold the points for each range and initialize
; them to missing values.  We want to use num_distinct_markers
; different colors, so we need num_distinct_markers sets of X and
; Y points.
;
  num_distinct_markers = nlevels+1        ; number of distinct markers
  lat_new = new((/num_distinct_markers,npts/),float,-999)
  lon_new = new((/num_distinct_markers,npts/),float,-999)
;
; Group the points according to which range they fall in. At the
; same time, create the label that we will use later in the labelbar
;       
  do i = 0, num_distinct_markers-1
    if (i.eq.0) then
      indexes = ind(R.lt.levels(0))
    end if
    if (i.eq.num_distinct_markers-1) then
      indexes = ind(R.ge.max(levels))
    end if
    if (i.gt.0.and.i.lt.num_distinct_markers-1) then       
      indexes = ind(R.ge.levels(i-1).and.R.lt.levels(i))
    end if
;
; Now that we have the set of indexes whose values fall within 
; the given range, take the corresponding lat/lon values and store
; them, so later we can color this set of markers with the appropriate
; color.
;
    if (.not.any(ismissing(indexes))) then 
      npts_range = dimsizes(indexes)   ; # of points in this range.
      lat_new(i,0:npts_range-1) = lat(indexes)
      lon_new(i,0:npts_range-1) = lon(indexes)
    end if
    delete(indexes)            ; Necessary b/c "indexes" may be a different
                               ; size next time.
  end do
  fn = addfile("./ter2.nc","r")
  hgt=fn->btdata(:,:)
  hgt = hgt
;===========================================================================
; Begin plotting section.
;
  wks = gsn_open_wks(outtype,outname)              ; send graphics to PNG file

;---Set up some map resources.
  mpres                       = True
  mpres@gsnMaximize           = True             ; Maximize plot in frame.
  mpres@gsnDraw               = False            ; Will draw later
  mpres@gsnFrame              = False            ; Don't advance the frame
  mpres@pmTickMarkDisplayMode = "Always"
;
;;---Zoom in on United States.
  mpres@mpMinLatF             = 15
  mpres@mpMaxLatF             = 55;70.
  mpres@mpMinLonF             = 73;50.
  mpres@mpMaxLonF             = 135;150.
  mpres@mpCenterLonF          = 105;100
;  mpres@tiMainString          = title+" ( "+dimsizes(lon)+" )" 
;  mpres@tiMainFontHeightF           = 0.015
  mpres@gsnLeftString         =""
  mpres@gsnRightString        =""
  mpres@gsnCenterString       =title+" ( "+dimsizes(lon)+" )"
  mpres@tmXBMajorOutwardLengthF =-0.001
;  mpres@tmXBTickSpacingF   = 5 
  mpres@gsnCenterStringOrthogonalPosF =0.01
  mpres@gsnRightStringFontHeightF=0.000001
  mpres@gsnStringFontHeightF  = 0.0000001
  mpres@gsnCenterStringFontHeightF=0.015
  mpres@gsnLeftStringFontHeightF=0.0000001
;  mpres@mpDataBaseVersion     = "MediumRes"
;  mpres@mpDataSetName         = "./database/Earth..4"
;  ;mpres@mpOutlineBoundarySets = "National"
;  mpres@mpOutlineSpecifiers   = (/ "China"/);, "China:Provinces"/)
;  mpres@mpOutlineOn           = True
  ;mpres@mpAreaMaskingOn            = True
  mpres@mpOutlineBoundarySets      = "NoBoundaries"

  mpres@cnLevelSelectionMode = "ExplicitLevels"   ; set explicit contour levels
  mpres@cnLevels    = (/ 0,100,200,500,1000,2000,3000,4000,5000/)   ; set levels

  ;mpres@mpOceanFillColor      = "dodgerblue";dodgerblue
  ;mpres@mpInlandWaterFillColor= "dodgerblue";"lightblue"
  ;mpres@mpLandFillColor       = "green4"
  mpres@mpFillOn               = False
;  map = gsn_csm_map(wks,mpres)
;  cmap      = read_colormap_file("terrain");topo_15lev");MPL_gist_earth");MPL_terrain");OceanLakeLandSnow")
   cmap       =read_colormap_file("OceanLakeLandSnow")
;  mpres@gsnSpreadColorStart = 12 
;  mpres@gsnSpreadColorEnd  =126
   cmap(:,3) = 0.5     ;     ; half transparency
 ; mpres@cnLevelSpacingF = 500
  mpres@cnFillOn              = True            ; color plot desired
  mpres@lbLabelsOn            = False
  mpres@cnLineLabelsOn        = False
  mpres@cnFillPalette         = cmap
  mpres@cnLinesOn             = False           ; turn off contour lines
  mpres@lbLabelBarOn   = False 
;  mpres@cnFillOpacityF =1.0 
  map = gsn_csm_contour_map(wks,hgt,mpres)

  attach_china_map(wks,map)
;  map = gsn_add_shapefile_polylines(wks,map,"./china/china_country.shp",lnres)

;--Create logical variables to hold the marker resources.
  gsres               = True
  gsres@gsMarkerIndex = 16          ; Use filled dots for markers.
;
; Loop through each grouping of markers, and draw them one set at
; a time, assigning the proper color and size with gsn_marker.
;
  base_size = 0.01
  pmid = new(num_distinct_markers,graphic)
  suon=abs(floattoint(log10(dimsizes(lon))))
  xishu=1.4 
  do n=0,suon
     xishu=xishu/1.4
  end do 
;  print(xishu)
  do i = 0, num_distinct_markers-1
    if (.not.ismissing(lat_new(i,0)))
      gsres@gsMarkerColor      = colors(i,:)
      gsres@gsMarkerSizeF      = base_size*1.*xishu ;* (i+1)/9.
      gsres@gsMarkerThicknessF = 1.*xishu
      pmid(i) = gsn_add_polymarker(wks,map,lon_new(i,:),lat_new(i,:),gsres)
    end if
  end do
  shapefile_root="./china"
  res = True
  res@gsLineThicknessF = 0.65
  res@gsLineOpacityF   = 0.5
  map@simplied_china_map1 = gsn_add_shapefile_polylines(wks, map, shapefile_root + "/simplied_china_country.shp", res)
  map@china_map1 = gsn_add_shapefile_polylines(wks, map, shapefile_root + "/china.shp", res)

  opt           = True
  cols          = (/188,215,240,0.6/)
  opt@color     =  "dodgerblue" ;cols(0)
  opt@thickness = 3 
  ;opt@dashP    = 1 
  ;add_Changjiang_Huanghe(wks,map,opt)

;---Draw labelbar and advance frame.
;  add_labelbar(wks,map,levels,colors)
  lbres  = True
  nlevels = dimsizes(levels)
  lbres                    = True
  lbres@vpWidthF           = 0.80             ; width
  lbres@vpHeightF          = 0.10             ; height
  lbres@lbPerimOn          = False            ; Turn off perimeter.
  lbres@lbOrientation      = "Horizontal"     ; Default is vertical.
  lbres@lbLabelAlignment   = "InteriorEdges"  ; Default is "BoxCenters"
  lbres@lbFillColors       = colors           ; Colors for boxes.
  lbres@lbMonoFillPattern  = True             ; Fill them all solid.
  lbres@lbLabelFontHeightF = 0.012            ; label font height
  labels = sprinti("%4.1i",levels)
  lbid = gsn_create_labelbar(wks,nlevels+1,labels,lbres)
  amres                  = True
  amres@amJust           = "TopCenter"
  amres@amParallelPosF   =  0.0   ; keep labelbar centered
  amres@amOrthogonalPosF =  0.55   ; move down and outside of plot
  tmpid1 = "anno"+unique_string("id")
  tmpid2 = "lbar"+unique_string("id")
  map@$tmpid1$ = gsn_add_annotation(map,lbid,amres)
  map@$tmpid2$ = lbid

  draw(map)
  frame(wks)
 system("convert "+outname+"."+outtype+" -trim +repage fso2."+outtype+";mv fso2."+outtype+" "+outname+"."+outtype)
end
;----------------------------------------------------------------------
