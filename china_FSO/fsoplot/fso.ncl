;************************************************
;
; These files are loaded by default in NCL V6.2.0 and newer
 load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"   
 load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"    
 load "./var.ncl"
;************************************************
begin
;  tn = (/-21997.7, -2002.5, -1959.83, -1526.91, -468.518, -414.135, -196.357, -160.771, -48.8726, 8.34431/)
;  ts =(/"sound", "qscat", "geoamv", "synop", "pilot", "sonde_sfc", "metar", "profiler", "ships", "buoy"/)
 dimtn = dimsizes(tn)         ; number of dates
;*********************************
; create plot
;********************************
  wks = gsn_open_wks(outtype,outname)
;;;; plot the impact for each instrument
  res1                            = True
  res1@gsnMaximize                = True    ; Maximize plot in frame
  res1@gsnDraw                    = False   ; Don't draw
  res1@gsnFrame                   = False   ; Don't advance frame
  res1@vpWidthF                   = 0.6     ; Make long and
  res1@vpHeightF                  = 0.4     ; narrow
;---Set axes limits. Add extra space for X max.
  if max(tn).eq.0 then
     if min(tn).eq.0 then
        exit
     end if
  end if
  if max(tn).le.0 then
     res1@trXMaxF                    = 0 
     res1@trXMinF                    = min(tn)*1.05
  else
     if min(tn).ge.0 then
         res1@trXMaxF                    = max(tn)*1.05
         res1@trXMinF                    =0
     else
         res1@trXMaxF                    = max(tn)*1.05
         res1@trXMinF                    = min(tn)*1.05
     end if
  end if
  print(res1@trXMinF)
  print(res1@trXMaxF)
  res1@trYMinF                    = 0
  res1@trYMaxF                    = dimtn+1 
  res1@gsnTickMarksPointOutward   = True
;---Put variable labels on Y axis
  res1@tmYUseLeft                 = False   ; Make right axis independent of left
  res1@tmYLOn                     = False   ; Turn off left tickmarks
  res1@tmYROn                     = True    ; Turn on right tickmarks
  res1@tmYLLabelsOn               = False   ; Turn off left labels
  res1@tmYRLabelsOn               = True    ; Turn on right labels
  res1@tmYRMinorOn                = False   ; Turn off minor ticks on Y axis
  res1@tmYRLabelFontHeightF       = 0.01   ; Increase font height
  res1@tmYRLabelDeltaF            = 0.1     ; Increase space b/w ticks and labels
;  res1@tmYRLabelJust              = "CenterRight"   ; right-justify labels
  res1@tmYRMode                   = "Explicit"
  res1@tmYRValues                 = ispan(1,dimtn,1)
  res1@tmYRLabels                 = ts 

;---Put variable labels on X axis
;  res1@tmXBMode                    = "Explicit"
;  res1@tmXBLabelsOn                = True    ; Turn on bottom labels
;  res1@tmXBValues                  = ispan(min_impact*scale,max_impact*scale,scale)
;  res1@tmXBLabels                  = res@tmXBValues    ; Turn on bottom labels
;  res1@tmXBMinorOn                 = True   ; Turn off minor ticks on Y axis
;  res1@tmXBMinorValues             = res@tmXBValues
  res1@tmXBLabelFontHeightF        = 0.01   ; Increase font height
  res1@tmXBLabelDeltaF             = 0.1     ; Increase space b/w ticks and labels
  res1@tmXBMajorLengthF            = 0.01
;  res1@tmXTOn                      = False   ; Turn off top tickmarks
  res1@tiXAxisString = "Forecast error contribution (J/kg)"  ; xaxis string
  res1@tiXAxisFontHeightF          = 0.015
;--Turn off Y axis tickmarks
  res1@tmYLMajorLengthF           = 0.
  res1@tmYLMajorOutwardLengthF    = 0.
  res1@tmXBMajorLengthF           = 0.01
  res1@tmXBMajorOutwardLengthF    = 0.01
  res1@tmXTOn                     = False   ; Turn off top tickmarks
  res1@tiMainString                = title ;"All Variables"
  res1@tiMainFontHeightF           = 0.015
;---Create blank plot without X grid lines
  plot_without_xgrid = gsn_blank_plot(wks,res1)
;---Create blank plot with X grid lines
  res1@tmXMajorGrid               = True    ; Turn on grid lines
  res1@tmXMajorGridLineDashPattern= 2       ; Dashed lines
  res1@tmXMajorGridThicknessF     = 1.0     ; default is 2 (?)
  plot_with_xgrid = gsn_blank_plot(wks,res1)
;---Arrays to hold the bar values and the bars themselves.
  xbar = new(5,float)
  ybar = new(5,float)
  dum3 = new(dimtn,graphic)

;---Set some resources for the bars.
  bres                            = True
  bres@gsEdgesOn                  = True      ; Outline the polygons (bars)
  do i=1,dimtn

;---Do longer bar first.
    if(tn(i-1).gt.0)then
       bres@gsFillColor = "firebrick3"
    else
       bres@gsFillColor = "dodgerblue3"
    end if
    xbar      = (/0,tn(i-1),tn(i-1),0,0/)
    ybar       =(/i-0.25*dimtn/5.,i-0.25*dimtn/5.,i+0.25*dimtn/5.,i+0.25*dimtn/5.,i-0.25*dimtn/5./)
    if dimtn.eq.1 then
       ybar       =(/i-0.075,i-0.075,i+0.075,i+0.075,i-0.075/)
    end if
        
    dum3(i-1) = gsn_add_polygon(wks,plot_without_xgrid,xbar,ybar,bres)
  end do

;
; Drawing the plot will draw all the attached bars. Have to draw
; the plot twice: one with the X grid lines and one without, in
; order to make sure the X grid lines are behind the bars.
;
; Couldn't see how else to do this.
;
  draw(plot_with_xgrid)
  draw(plot_without_xgrid)
  frame(wks)
  system("convert "+outname+".png -trim +repage fso2.png;mv fso2.png "+outname+".png")
end

