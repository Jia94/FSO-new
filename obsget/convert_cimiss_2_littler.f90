  program convert_cimiss_2_littler

!=========================================================================
! Yonghui Weng, LongRun @ 20171229
!
! command convention
! convert_cimiss_2_littler.exe -i input_files -o output_file
!  result: convert all input_files to one output file.  
!
! input_files can be: 
!             dropsondes in xml, e.g., 2017111500.xml, 
!             metar in xml, e.g., 201712260000.xml
!             profiler in txt, e.g., Z_RADA_51463_WPRD_MOC_NWQC_ROBS_LC_QI_20171010000000.TXT
!
! Examples:
!    convert_cimiss_2_littler.exe -i /home/test/2017111500.xml:201712260000.xml:/folder/Z_RADA_51463_WPRD_MOC_NWQC_ROBS_LC_QI_20171010000000.TXT -o test_cimiss_little_r
!
!=========================================================================
  implicit none

  integer, parameter  :: unit_in  = 10  ! unit_in for input little_r files
  integer, parameter  :: unit_out = 20  ! unit_out for output little_r file

  integer             :: i, j, k, iargc, nfl, nf, iost, iread
  character (len=50)  :: arg   ! -i or -o
  character (len=50000) :: arg1, input_files
  character (len=500)  :: input_file, output_file
  character (len=2000) :: line
  integer, dimension(2,5000) :: flposition  ! 1: start, 2: end
!----------------------------------------------------------------
! 1.0 --- get argc
! like this way:  convert_cimiss_2_littler.exe -i file1:file2 -o file3
  if (iargc() .lt. 4) then
     write(*,*)' usage: convert_cimiss_2_littler.exe -i input_files -o output_file'
     stop
  else
     do i = 1, iargc(), 2 
        call getarg(i, arg)
        call getarg(i+1, arg1)
        select case (arg)
               case ('-i', '--input')
                    input_files=trim(arg1)
               case ('-o', '--output')
                    output_file=trim(arg1)
        end select
     enddo
  endif

!----------------------------------------------------------------
! 2.0 --- split input files
  k=1
  nfl = 0
  do_split_inputs_loop: do
     !write(*,*)'k:len_trim(input_files)=',k,len_trim(input_files)
     j = index(input_files(k:len_trim(input_files)),':')
     if ( j == 0 ) then
        nfl = nfl + 1
        flposition(1, nfl) = k
        flposition(2, nfl) = len_trim(input_files)
        exit do_split_inputs_loop
     else
        nfl = nfl + 1
        flposition(1, nfl) = k
        flposition(2, nfl) = k-1+j-1
        k=k+j
     endif
     if ( k >= len_trim(input_files) ) exit do_split_inputs_loop
  enddo do_split_inputs_loop
        
!----------------------------------------------------------------
! 3.0 --- open output file
  open(unit = unit_out , file = trim(output_file))

!----------------------------------------------------------------
! 4.0 --- input_files loop
  do_input_files_loop: do nf = 1, nfl
     input_file = input_files(flposition(1,nf):flposition(2,nf))

     iread=0
     open(unit = unit_in, file=trim(input_file), status='old', form = 'formatted', iostat = iost )
     if ( iost .ne. 0 ) then
        write(*,*)'!!!! CANNOT OPEN FILE: '//trim(input_file)//' !!!!'
     else
        read(unit_in, '(a1500)', end=999)line
        if ( line(1:7) == 'WNDHOBS' ) then  ! profiler in text
           close (unit_in)
           call convert_profiler_to_littler(input_file, unit_out)
           iread=1
        else if ( index(trim(line),'xml') > 0 ) then   ! xml file for metar + dropsonde      
           read(unit_in, '(a1500)')line
           close (unit_in)
           if ( index(trim(line),'UPAR_CHN_MUL_FTM') > 0 ) then  ! xml for dropsonde
              call convert_dropsonde_to_littler(input_file, unit_out)
              iread=1
           else if ( index(trim(line),'SURF_CHN_MAIN_MIN') > 0 ) then  ! xml for metar
              call convert_metar_to_littler(input_file, unit_out)
              iread=1
           endif
        endif
        if ( iread == 0 ) then
           write(*,*) "!!! UNKNOWN FILE FORMAT: "//trim(input_file)//' !!!!'
        endif
999     continue
     endif

  enddo do_input_files_loop
  
  close (unit_out)

  end program
   
!========================================================================================
  subroutine convert_profiler_to_littler(input_file, unit_out)
!
! one file has only one station

  implicit none

  character (len=*), intent(in) :: input_file
  integer, intent(in)           :: unit_out

  character (len=80)  :: line
  integer :: unit_in, kx, iost, kcount, uvqc, i, j, k
  integer, parameter  :: kmax = 500
  
  character (len=5)  :: station_id
  real               :: station_lon, station_lat, station_elv
  character (len=2)  :: station_type
  character (len=14) :: mdate
  character (len=6)  :: fm
  character (len=40) :: string1, string2 , string3 , string4
  real,dimension(500,4) :: dat  !hgt, dir, spd, missing

  real               :: dir, spd
  real               :: missing

  data missing /-888888./

  dat = missing
  string1(1:40)=' '
  string2(1:40)=' '
  string3(1:40)=' '
  string4(1:40)=' '

  open(unit = unit_in, file=trim(input_file), status='old', form = 'formatted', iostat = iost )
  if ( iost .ne. 0 ) return

  !-- 1st line: WNDHOBS 01.20
  read(unit_in, '(a80)', iostat = iost)line 
  if ( line(1:7) /= 'WNDHOBS' .or. iost .ne. 0 ) return

  !--- station loop
  do_station_loop : do
     !-- 2nd line: 51463 0087.6458 043.7889 00935.0 LC 20171010000000 00
     read(unit_in,'(a5,f10.4,f9.4,f8.1,1x,a2,1x,a14)',iostat=iost) station_id, station_lon,  &
                  station_lat, station_elv, station_type, mdate
     if ( iost .ne. 0 ) return
     write(*,'(a5, 3f8.2)')station_id, station_lon, station_lat, station_elv

     !-- 3rd line: HOBS
     read(unit_in, '(a4)', iostat = iost)line
     if ( iost .ne. 0 ) return
     if ( line(1:4) == "HOBS" ) then
        fm="FM-132"
     else
        write(*,*)"unkown format for :"//trim(input_file)
        return
     endif
     write(*,*)fm

     !--read other lines
     kcount=0
     do_read_robs_line: do
        read(unit_in, '(a41)', iostat = iost)line
        if ( iost .ne. 0 ) exit do_read_robs_line

        if ( line(1:4) == "NNNN" ) exit do_read_robs_line
        if ( line(7:41) == "///// ///// ////// /// /// ////////" ) cycle do_read_robs_line

        !---readin data
        read(line(1:5),'(i5)',iostat=iost)i
        if ( iost .ne. 0 ) cycle do_read_robs_line
        if ( i<0 .or. i>30000 ) cycle do_read_robs_line
     
        read(line(7:11),'(f5.1)',iostat=iost)dir 
        if ( iost .ne. 0 ) dir=missing
        read(line(13:17),'(f5.1)',iostat=iost)spd 
        if ( iost .ne. 0 ) spd=missing
        read(line(26:28),'(i3)',iostat=iost)uvqc
        if ( iost .ne. 0 .or. uvqc<=0 ) cycle do_read_robs_line

        !---processing
        if ( dir>=0. .and. dir<=360. .and. spd<300. .and. uvqc >0 ) then
           kcount=kcount+1
           !write(*,'(i5, 2f6.1,i4)')i, dir, spd, uvqc
           dat(kcount,1) = i*1.0 + station_elv
           dat(kcount,2) = dir
           dat(kcount,3) = spd
        endif
  
     enddo do_read_robs_line
     write(*,*)kcount
     string1(3:18)="WNDHOBS profiler"
     string2(6:10)=station_id
     string3=fm//' profl     '
     string4='  '

     call write_obs (dat(1:kcount,4), dat(1:kcount,1), dat(1:kcount,4), dat(1:kcount,4), dat(1:kcount,3), dat(1:kcount,2), &
                     missing, station_elv, station_lat, station_lon, mdate, kcount,                                        &
                     string1, string2, string3, string4, .false., 0, unit_out)
  enddo do_station_loop

  end subroutine convert_profiler_to_littler

!========================================================================================
  subroutine convert_dropsonde_to_littler(input_file, unit_out)

  implicit none

  character (len=*), intent(in) :: input_file
  integer, intent(in)           :: unit_out

  integer, parameter  :: maxsta=500,  & ! station num
                         maxlev=2000    ! max vertical levels 
  real, parameter     :: missing=-888888.

  character (len=5)  :: Station_Id_C
  character (len=5), dimension(maxsta) :: sta_id
  integer, dimension(maxsta) :: sta_lev
  real,dimension(maxsta)     :: sta_lat, sta_lon, sta_elv, sta_sfcp, sta_slp 
  character (len=14), dimension(maxsta) :: mdate, odate
  real,dimension(maxlev,maxsta) :: sta_p, sta_z, sta_t, sta_td, sta_dir, sta_spd
  
  character (len=4000)  :: line
  integer :: unit_in, kx, iost, nsta, i, j, k, point_sta, nrecord, idat
  integer :: iyear, imon, iday, ihour, imin, isecond
  real    :: fdat
  character (len=25) :: temp_c
  character (len=40) :: string1, string2 , string3 , string4

  open(unit = unit_in, file=trim(input_file), status='old', form = 'formatted', iostat = iost )
  if ( iost .ne. 0 ) return

  !-- 1st line: xml head
  read(unit_in, '(a80)')line
  !write(*,*)index(trim(line),'xml')
  if ( index(trim(line),'xml') <= 0 ) then
     write(*,*)'!!!! FILE '//trim(input_file)//' is not an xml file!!!!'
     return
  endif

  !--- line loop
  sta_lev=0
  nrecord=0
  nsta=0  !count for station
  do_dropsonde_record_loop : do
     read(unit_in, '(a4000)', iostat = iost)line
     if ( iost .ne. 0 ) exit do_dropsonde_record_loop

     if ( index(trim(line),'<DS') > 0 ) then
        write(*,*)line(1:80)
     elseif ( index(trim(line),'/DS') > 0 ) then
        write(*,*)trim(line)
     elseif ( index(trim(line),'<R') > 0 ) then
        nrecord=nrecord+1

        !---replace '"'
        do j = 5, index(line,'>')
           if ( line(j:j) == '"' ) line(j:j)=' '
        enddo

        !---station_id
        i=index(trim(line),'Station_Id_C')
        read(line(i:i+50),*)temp_c, Station_Id_C
        if ( nsta < 1 ) then
           nsta=nsta+1
           sta_id(nsta)=Station_Id_C
           !write(*,*)'1st  ', temp_c,'  is  ', Station_Id_C
        else
           point_sta=0
           do_lookup_station: do j = 1, nsta
              if ( Station_Id_C == sta_id(j) ) then
                 point_sta=j
                 exit do_lookup_station
              endif
              !write(*,*) nsta, point_sta, '   ', Station_Id_C, sta_id(j)
           enddo do_lookup_station
           !write(*,*) 'point_sta=',point_sta
           if (point_sta<1) then
              nsta=nsta+1
              sta_id(nsta)=Station_Id_C
              !write(*,*)temp_c,'  is  ', Station_Id_C
           endif
        endif

        !---processing station information
        !---lat
        i=index(trim(line),'Lat=')
        read(line(i:i+50),*)temp_c, sta_lat(nsta)

        !---lon
        i=index(trim(line),'Lon=')
        read(line(i:i+50),*)temp_c, sta_lon(nsta)

        !---elv
        i=index(trim(line),' Alti=')
        read(line(i:i+50),*)temp_c, sta_elv(nsta)

        !---epoch time and observation time
        i=index(trim(line),'Year_Data=')
        read(line(i:i+15),*)temp_c, iyear
        i=index(trim(line),'Mon_Data=')
        read(line(i:i+15),*)temp_c, imon
        i=index(trim(line),'Day_Data=')
        read(line(i:i+15),*)temp_c, iday
        i=index(trim(line),'Hour_Data=')
        read(line(i:i+15),*)temp_c, ihour
        write(mdate(nsta),'(i4.4,3i2.2,a4)')iyear, imon, iday, ihour, "0000"

        i=index(trim(line),' Year=')
        read(line(i:i+15),*)temp_c, iyear
        i=index(trim(line),' Mon=')
        read(line(i:i+15),*)temp_c, imon
        i=index(trim(line),' Day=')
        read(line(i:i+15),*)temp_c, iday
        i=index(trim(line),' Hour=')
        read(line(i:i+15),*)temp_c, ihour
        i=index(trim(line),' Min=')
        read(line(i:i+15),*)temp_c, imin
        i=index(trim(line),' Second=')
        read(line(i:i+15),*)temp_c, isecond
        write(odate(nsta),'(i4.4,5i2.2)')iyear, imon, iday, ihour, imin, isecond

        !---processing station information
        i=index(trim(line),' PRS_HWC=')
        if ( i>0 ) then
           !write(*,'(a16)')line(i:i+15)
           read(line(i:i+15),*)temp_c, fdat
        endif
        i=index(trim(line),' GPH=')
        if ( i>0 ) then
           read(line(i:i+15),*)temp_c, idat
        endif
        if (fdat > 1200 .and. idat > 35000 ) cycle do_dropsonde_record_loop

        sta_lev(nsta)=sta_lev(nsta)+1

        sta_p(sta_lev(nsta),nsta)=missing
        sta_z(sta_lev(nsta),nsta)=missing
        if ( fdat > 0 .and. fdat < 1200 ) sta_p(sta_lev(nsta),nsta)=fdat*100.0 ! hpa to pa
        if ( idat >=0 .and. idat < 30000 ) sta_z(sta_lev(nsta),nsta)=idat*1.0
      
        sta_t(sta_lev(nsta),nsta)=missing
        i=index(trim(line),' TEM=')
        if ( i>0 ) then
           read(line(i:i+15),*)temp_c, fdat
           if ( fdat >=-100. .and. fdat < 65. ) sta_t(sta_lev(nsta),nsta)=fdat+273.15
        endif
      
        sta_td(sta_lev(nsta),nsta)=missing
        i=index(trim(line),' DPT=')
        if ( i>0 ) then
           read(line(i:i+15),*)temp_c, fdat
           if ( fdat >=-150. .and. fdat < 65. ) sta_td(sta_lev(nsta),nsta)=fdat+273.15
        endif
      
        sta_dir(sta_lev(nsta),nsta)=missing
        i=index(trim(line),' WIN_D=')
        if ( i>0 ) then
           read(line(i:i+15),*)temp_c, idat
           if ( idat >=0 .and. idat <= 360 ) sta_dir(sta_lev(nsta),nsta)=idat*1.0
        endif
      
        sta_spd(sta_lev(nsta),nsta)=missing
        i=index(trim(line),' WIN_S=')
        if ( i>0 ) then
           read(line(i:i+15),*)temp_c, idat
           if ( idat >=0 .and. idat < 300 ) sta_spd(sta_lev(nsta),nsta)=idat*1.0
        endif

     endif 

  enddo do_dropsonde_record_loop
  write(*,*)nsta, nrecord
  do j = 1, nsta
     !write(*,'(a5,3f8.1, 2(1x, a14), i6)')sta_id(j), sta_lat(j), sta_lon(j), sta_elv(j),mdate(j),odate(j), sta_lev(j)
     string1(1:40)=' '
     string2(1:40)=' '
     string3(1:40)=' '
     string4(1:40)=' '
     
     string1(3:18)="UPAR_CHN_MUL_FTM"
     string2(6:10)=sta_id(j)
     string3='FM-37 DROPSONDE'
     
     call write_obs (sta_p(1:sta_lev(j),j), sta_z(1:sta_lev(j),j), sta_t(1:sta_lev(j),j),       &
                     sta_td(1:sta_lev(j),j), sta_spd(1:sta_lev(j),j), sta_dir(1:sta_lev(j),j),  &
                     missing, sta_elv(j), sta_lat(j), sta_lon(j), odate(j), sta_lev(j),         &
                     string1, string2, string3, string4, .false., 0, unit_out)
  enddo

  end subroutine convert_dropsonde_to_littler

!========================================================================================
  subroutine convert_metar_to_littler(input_file, unit_out)

  implicit none

  character (len=*), intent(in) :: input_file
  integer, intent(in)           :: unit_out

  real, parameter     :: missing=-888888.

  character (len=40)    :: string1, string2 , string3 , string4
  real                  :: sta_lat, sta_lon, sta_elv
  real, dimension(1)    :: sta_p, sta_z, sta_t, sta_td, sta_rh, sta_dir, sta_spd

  character (len=2000)  :: line
  integer               :: unit_in, iost, i, j, k, i1, nsta
  integer, dimension(2) :: ipst
  character (len=6)     :: sta_idd
  character (len=5)     :: sta_idc
  character (len=25)    :: temp_c
  integer               :: iyear, imon, iday, ihour, imin, isecond
  character (len=14)    :: mdate
  real                  :: vapor

  open(unit = unit_in, file=trim(input_file), status='old', form = 'formatted', iostat = iost )
  if ( iost .ne. 0 ) return

  !-- 1st line: xml head
  read(unit_in, '(a80)')line
  if ( index(trim(line),'xml') <= 0 ) then
     write(*,*)'!!!! FILE '//trim(input_file)//' is not an xml file!!!!'
     return
  endif

  !--- line loop
  nsta=0
  do_metar_record_loop : do
     read(unit_in, '(a2000)', iostat = iost)line
     if ( iost .ne. 0 ) exit do_metar_record_loop

     if ( index(trim(line),'<DS') > 0 ) then
        write(*,*)" --- start METAR processing ---"
     elseif ( index(trim(line),'/DS') > 0 ) then
        write(*,*)" --- end METAR processing ---"
        exit do_metar_record_loop
     elseif ( index(trim(line),'<R') > 0 ) then
        nsta=nsta+1
        string1(1:40)=' '
        string2(1:40)=' '
        string3(1:40)=' '
        string4(1:40)=' '

        !--- Station_Name
        i=index(trim(line),'Station_Name=')
        !write(*,'(a13,i2)')"Station_Name=",i
        ipst(1:2)=-1
        i1=0
        do_find_station_name: do k = i, i+30
           if ( line(k:k) == '"' ) then
              i1=i1+1
              ipst(i1)=k
              if (i1 == 2) exit do_find_station_name
           endif
        enddo do_find_station_name
        if ( ipst(1) > 1 .and. ipst(2) > ipst(1) .and. ipst(2)< 500 ) then
           string1="SURF_CHN_MAIN_MIN  "//line(ipst(1)+1:ipst(2)-1)
        else
           string1="SURF_CHN_MAIN_MIN  "
        endif

        !---replace '"'
        do j = 5, index(line,'>')
           if ( line(j:j) == '"' ) line(j:j)=' '
        enddo

        !---station id
        i=index(trim(line),'Station_Id_C')
        read(line(i:i+50),*)temp_c, sta_idc
        i=index(trim(line),'Station_Id_d')
        read(line(i:i+50),*)temp_c, sta_idd
        string2=sta_idc//' '//sta_idd

        string3='FM-15 METAR'
        !write(*,*)trim(string1)//" === "//trim(string2)//" === "//trim(string3)

        !---lat, lon, elv
        i=index(trim(line),'Lat=')
        read(line(i:i+50),*)temp_c, sta_lat
        i=index(trim(line),'Lon=')
        read(line(i:i+50),*)temp_c, sta_lon
        i=index(trim(line),' Alti=')
        read(line(i:i+50),*)temp_c, sta_elv 

        !---observation time
        i=index(trim(line),' Year=')
        read(line(i:i+15),*)temp_c, iyear
        i=index(trim(line),' Mon=')
        read(line(i:i+15),*)temp_c, imon
        i=index(trim(line),' Day=')
        read(line(i:i+15),*)temp_c, iday
        i=index(trim(line),' Hour=')
        read(line(i:i+15),*)temp_c, ihour
        i=index(trim(line),' Min=')
        read(line(i:i+15),*)temp_c, imin
        write(mdate,'(i4.4,5i2.2)')iyear, imon, iday, ihour, imin, 0
        !write(*,'(3f8.1,2x,a14)')sta_lat, sta_lon, sta_elv, mdate

        !---data
        i=index(trim(line),' PRS=')
        read(line(i:i+50),*)temp_c, sta_p(1)
        if ( sta_p(1) > 5. .and. sta_p(1) < 1020. ) then
           sta_p(1)=sta_p(1)*100.  !hpa to pa
        else
           sta_p(1)=missing
        endif

        i=index(trim(line),' PRS_Sensor_Alti=')
        read(line(i:i+50),*)temp_c, sta_z(1)
        if ( sta_z(1) < -100. .or. sta_z(1) > 8000. ) sta_z(1)=sta_elv + 1.0 ! given 1m above
        if ( sta_z(1) < -100. .or. sta_z(1) > 8000. ) sta_z(1)=missing 
        
        i=index(trim(line),' TEM=')
        read(line(i:i+50),*)temp_c, sta_t(1)
        if ( sta_t(1) > -150. .and. sta_t(1) < 80. ) then
           sta_t(1)=sta_t(1)+273.15
        else
           sta_t(1)=missing
        endif 

        i=index(trim(line),' RHU=')
        read(line(i:i+50),*)temp_c, sta_rh(1)
        if ( sta_rh(1) > 0. .and. sta_rh(1) <= 80. ) then
           if ( sta_t(1) > 100. .and. sta_t(1) < 350. ) then
              vapor = (sta_rh(1)/100.)*611.2 * exp(17.67*(sta_t(1)-273.15)/(sta_t(1)-273.15+243.5))           
              sta_td(1) = 243.5 * log(vapor/611.2)/(17.67-log(vapor/611.2)) + 273.15
           else
              sta_td(1)=missing
           endif
        else
           sta_rh(1)=missing
           sta_td(1)=missing
        endif

        i=index(trim(line),' WIN_D_Avg_1mi=')
        read(line(i:i+50),*)temp_c, sta_dir(1)
        if ( sta_dir(1) > 360. .or. sta_dir(1) < 0 ) sta_dir(1)=missing

        i=index(trim(line),' WIN_S_Avg_1mi=')
        read(line(i:i+50),*)temp_c, sta_spd(1)
        if ( sta_spd(1) > 200. .or. sta_spd(1) < 0 ) sta_spd(1)=missing
        
        write(*,'(7f10.1)')sta_p(1), sta_z(1), sta_t(1), sta_td(1), sta_rh(1), sta_dir(1), sta_spd(1)

        call write_obs (sta_p, sta_z, sta_t, sta_td, sta_spd, sta_dir,  &
                        missing, sta_elv, sta_lat, sta_lon, mdate, 1,   &
                        string1, string2, string3, string4, .false., 0, unit_out)

        !if (nsta > 10) exit do_metar_record_loop

     else
        write(*,*)" --- dont know format ---"
     endif

  enddo do_metar_record_loop

  end subroutine convert_metar_to_littler 

!========================================================================================
  subroutine write_obs ( p , z , t , td , spd , dir ,  & 
                         slp , ter , xlat , xlon , mdate , kx ,  & 
                         string1 , string2 , string3 , string4 , & 
                         bogus , iseq_num ,  iunit )

  implicit none

  real, dimension(kx), intent(in)  :: p , z , t , td , spd , dir
  real, intent(in)                 :: slp, ter, xlat, xlon
  character(len=40), intent(in)    :: string1, string2 , string3 , string4
  character(len=14), intent(in)    :: mdate
  logical, intent(in)              :: bogus
  integer, intent(in)              :: kx, iseq_num, iunit 

  character(len=20)   :: date_char
  character(len=84)   :: ini_format
  character(len=22)   :: mid_format
  character(len=14)   :: end_format

  integer, allocatable, dimension(:)  :: indx

  logical     :: is_sounding, discard
  integer     :: k

  data is_sounding /.false./
  data discard /.false./

! little_r platforms
!
!....  platforms
!
!   Name    WMO Codes     WMO Code names
!   synop    12,14       'SYNOP','SYNOP MOBIL'                      ! Land!   synoptic reports
!   ship     13          'SHIP'                                     ! Ship and!   moored buoy synoptic reports
!   metar    15,16       'METAR','SPECI'
!   buoy     18          'BUOY'                                     ! Drifting!   buoy reports
!   pilot    32,33,34    'PILOT','PILOT SHIP','PILOT MOBIL'         ! Upper air!   wind profiles: UG UH UI UP UQ  UY
!   sound    35,36,37,38 'TEMP','TEMP SHIP, 'TEMP DROP','TEMP MOBIL'! Upper air!   temperature, humidity and wind profiles: UE UF UK UM US
!   amdar    42          'AMDAR'                                    ! Aircraft!   Meteorological Data And Reporting Relay: UD
!   satem    86          'SATEM'
!   satob    88          'SATOB'                                    ! satwind:!   JMA satob-coded winds
!   airep    96,97       'AIREP'                                    ! Aircraft!   reports: UA UB
!   gpspw    111         'GPSPW'
!   gpsztd   114         'GPSZD'
!   gpsref   116         'GPSRF'
!   gpseph   118         'GPSEP'
!   ssmt1    121         'SSMT1'
!   ssmt2    122         'SSMT2'
!   ssmi     125,126     'SSMI'                 ! Micro-wave!   radiances from Special Sensor Microwave Imager on DMSP satellitees
!   tovs     131         'TOVS'                 ! TIROS!   Operational Vertical Sounder
!   qscat    281         'Quikscat'             ! Seawinds!   data obtained from QuikSCAT
!   profl    132         'Profilers'            ! 
!   bogus    135         'BOGUS'
!   airs     133         'AIRSRET'
!   other Any other code 'UNKNOWN'

!.... reduced to BUFRLIB classes:
!   sounding  32,33,34,35,36,37,38,  UPPER-AIR (RAOB, PIBAL,!   RECCO, DROPS) REPORTS 
!   surface   12,14,                 SURFACE LAND (SYNOPTIC)!   REPORTS
!   metar     15,16,                 SURFACE LAND (METAR)!   REPORTS
!   profiler  132                    WIND PROFILER REPORTS
!   aircft    42,96,97               AIREP/PIREP, AMDAR!   (ASDAR/ACARS), E-ADAS (AMDAR BUFR) AIRCRAFT REPORTS
!   sfcshp    13,18,                 SURFACE MARINE (SHIP, BUOY,!   C-MAN PLATFORM) REPORTS
!   satwnd    88,                    SATELLITE-DERIVED WIND!   REPORTS
!   spssmi    125,126                DMSP SSM/I RETRIEVAL!   PRODUCTS (REPROCESSED WIND SPEED, TPW)

!    ... Define writing formats

!    ini_format writes an header:
!      two integers --> 2f20.5
!                       station latitude (north positive)
!                       station longitude (east positive)
!       string1, string2, string3, string4 --> 2a40 & 2a40
!                       string1 ID of station
!                       string2 Name of station
!                       string3 Description of the measurement device
!                       string4 GTS, NCAR/ADP, BOGUS, etc.
!       terrain elevation (m) --> 1f20.5
!       five integers: kx*6, 0, 0, iseq_num, 0 --> 5i10
!                       Number of valid fields in the report
!                       Number of errors encountered during the decoding of this observation
!                       Number of warnings encountered during decoding of this observation
!                       Sequence number of this observation
!                       Number of duplicates found for this observation
!       three logicals: is_sounding, bogus, .false. --> 3L10
!                       Multiple levels or a single level
!                       bogus report or normal one
!                       Duplicate and discarded (or merged) report
!       two integers --> 2i10
!                       Seconds since 0000 UTC 1 January 1970
!                       Day of the year
!       date of observation as character --> a20
!                       YYYYMMDDHHmmss
!       13 couples of numbers --> 13(f13.5,i7)
!                       1. Sea-level pressure (Pa) and a QC flag
!                       2. Reference pressure level (for thickness) (Pa) and a QC flag
!                       3. Ground Temperature (T) and QC flag
!                       4. Sea-Surface Temperature (K) and QC
!                       5. Surface pressure (Pa) and QC
!                       6. Precipitation Accumulation and QC
!                       7. Daily maximum T (K) and QC
!                       8. Daily minimum T (K) and QC
!                       9. Overnight minimum T (K) and QC
!                       10. 3-hour pressure change (Pa) and QC
!                       11. 24-hour pressure change (Pa) and QC
!                       12. Total cloud cover (oktas) and QC
!                       13. Height (m) of cloud base and QC
      ini_format =  ' ( 2f20.5 , 2a40 , ' & 
                  // ' 2a40 , 1f20.5 , 5i10 , 3L10 , '  & 
                  // ' 2i10 , a20 ,  13( f13.5 , i7 ) ) '

!    mid_format writes the actual observations:
!       ten floating numbers and integers --> 10( f13.5 , i7 )
!          1. Pressure (Pa) of observation, and QC
!          2. Height (m MSL) of observation, and QC
!          3. Temperature (K) and QC
!          4. Dewpoint (K) and QC
!          5. Wind speed (m s-1 ) and QC
!          6. Wind direction (degrees) and QC
!          7. U component of wind (m s-1 ), and QC
!          8. V component of wind (m s-1 ), and QC
!          9. Relative Humidity (%) and QC
!         10. Thickness (m), and QC
      mid_format =  ' ( 10( f13.5 , i7 ) ) '

!    end_format writes the tail of the little_r file
!       three integers --> 3 ( i7 )
!         Number of valid fields in the report
!         Number of errors encountered during the decoding of the report
!         Number of warnings encountered during the decoding the report
      end_format = ' ( 3 ( i7 ) ) ' 
!    ... End of writing formats

!     ... Init of date_char
      date_char(1:6)='      '
      date_char(7:20)=mdate(1:14)
!      write (*,*) 'date_char        -->',date_char
!     ****************************************
!    ... End of date_char

!    ... Write ini format (known as header-format)
      WRITE ( UNIT = iunit , ERR = 19 , FMT = ini_format )    & 
              xlat, xlon,                                     &
              string1, string2,  string3, string4,            &
              ter,                                            &
              kx*6, 0, 0, 0, 0,                               &
              is_sounding, bogus, discard,                    &
              -888888, -888888,                               &
              date_char,                                      &
              slp, 0,                                         &
                -888888.,0,-888888.,0,-888888.,0,p(1),0,      &
                -888888.,0,-888888.,0,-888888.,0,-888888.,0,  &
                -888888.,0,-888888.,0,-888888.,0,-888888.,0
     
!    ... Sort p(1:kx)
      allocate(indx(kx))
      if ( string3(1:6) == 'FM-132' ) then
         call quicksort(kx, z(1:kx), indx)
      else
         call quicksort(kx, p(1:kx), indx)
      endif
      

!    ... Write mid format (known as data-records)
      do 100 k = 1 , kx
         WRITE ( UNIT = iunit , ERR = 19 , FMT = mid_format ) & 
                p(indx(k)), 0, z(indx(k)),0, t(indx(k)),0, td(indx(k)),0,             &
                spd(indx(k)),0, dir(indx(k)),0,                           &
                -888888.,0, -888888.,0,                       &
                -888888.,0, -888888.,0
100   continue
      deallocate(indx)

!    ... Again write mid format (known as end-data-record)
      WRITE ( UNIT = iunit , ERR = 19 , FMT = mid_format )    &
       -777777.,0, -777777.,0, float(kx),0,                   &
       -888888.,0, -888888.,0, -888888.,0,                    &
       -888888.,0, -888888.,0, -888888.,0,                    &
       -888888.,0

!    ... Write end format
      WRITE ( UNIT = iunit , ERR = 19 , FMT = end_format )  kx, 0, 0

      return
19    continue
      print *,'troubles writing little_r observation'
      stop 19
  end subroutine write_obs 

!=======================================================================================

SUBROUTINE quicksort(n,x,ind)

IMPLICIT NONE

REAL, INTENT(IN)  :: x(n)
INTEGER, INTENT(IN OUT)   :: ind(n)
INTEGER, INTENT(IN)    :: n

!***************************************************************************

!                                                         ROBERT RENKA
!                                                 OAK RIDGE NATL. LAB.

!   THIS SUBROUTINE USES AN ORDER N*LOG(N) QUICK SORT TO SORT A REAL 
! ARRAY X INTO INCREASING ORDER.  THE ALGORITHM IS AS FOLLOWS.  IND IS
! INITIALIZED TO THE ORDERED SEQUENCE OF INDICES 1,...,N, AND ALL INTERCHANGES
! ARE APPLIED TO IND.  X IS DIVIDED INTO TWO PORTIONS BY PICKING A CENTRAL
! ELEMENT T.  THE FIRST AND LAST ELEMENTS ARE COMPARED WITH T, AND
! INTERCHANGES ARE APPLIED AS NECESSARY SO THAT THE THREE VALUES ARE IN
! ASCENDING ORDER.  INTERCHANGES ARE THEN APPLIED SO THAT ALL ELEMENTS
! GREATER THAN T ARE IN THE UPPER PORTION OF THE ARRAY AND ALL ELEMENTS
! LESS THAN T ARE IN THE LOWER PORTION.  THE UPPER AND LOWER INDICES OF ONE
! OF THE PORTIONS ARE SAVED IN LOCAL ARRAYS, AND THE PROCESS IS REPEATED
! ITERATIVELY ON THE OTHER PORTION.  WHEN A PORTION IS COMPLETELY SORTED,
! THE PROCESS BEGINS AGAIN BY RETRIEVING THE INDICES BOUNDING ANOTHER
! UNSORTED PORTION.

! INPUT PARAMETERS -   N - LENGTH OF THE ARRAY X.

!                      X - VECTOR OF LENGTH N TO BE SORTED.

!                    IND - VECTOR OF LENGTH >= N.

! N AND X ARE NOT ALTERED BY THIS ROUTINE.

! OUTPUT PARAMETER - IND - SEQUENCE OF INDICES 1,...,N PERMUTED IN THE SAME
!                          FASHION AS X WOULD BE.  THUS, THE ORDERING ON
!                          X IS DEFINED BY Y(I) = X(IND(I)).

!*********************************************************************

! NOTE -- IU AND IL MUST BE DIMENSIONED >= LOG(N) WHERE LOG HAS BASE 2.
! (OK for N up to about a billon)

!*********************************************************************

INTEGER   :: iu(21), il(21)
INTEGER   :: m, i, j, k, l, ij, it, itt, indx
REAL      :: r
REAL      :: t

! LOCAL PARAMETERS -

! IU,IL =  TEMPORARY STORAGE FOR THE UPPER AND LOWER
!            INDICES OF PORTIONS OF THE ARRAY X
! M =      INDEX FOR IU AND IL
! I,J =    LOWER AND UPPER INDICES OF A PORTION OF X
! K,L =    INDICES IN THE RANGE I,...,J
! IJ =     RANDOMLY CHOSEN INDEX BETWEEN I AND J
! IT,ITT = TEMPORARY STORAGE FOR INTERCHANGES IN IND
! INDX =   TEMPORARY INDEX FOR X
! R =      PSEUDO RANDOM NUMBER FOR GENERATING IJ
! T =      CENTRAL ELEMENT OF X

IF (n <= 0) RETURN

! INITIALIZE IND, M, I, J, AND R

DO  i = 1, n
  ind(i) = i
END DO
m = 1
i = 1
j = n
r = .375

! TOP OF LOOP

20 IF (i >= j) GO TO 70
IF (r <= .5898437) THEN
  r = r + .0390625
ELSE
  r = r - .21875
END IF

! INITIALIZE K

30 k = i

! SELECT A CENTRAL ELEMENT OF X AND SAVE IT IN T

ij = i + r*(j-i)
it = ind(ij)
t = x(it)

! IF THE FIRST ELEMENT OF THE ARRAY IS GREATER THAN T,
!   INTERCHANGE IT WITH T

indx = ind(i)
IF (x(indx) > t) THEN
  ind(ij) = indx
  ind(i) = it
  it = indx
  t = x(it)
END IF

! INITIALIZE L

l = j

! IF THE LAST ELEMENT OF THE ARRAY IS LESS THAN T,
!   INTERCHANGE IT WITH T

indx = ind(j)
IF (x(indx) >= t) GO TO 50
ind(ij) = indx
ind(j) = it
it = indx
t = x(it)

! IF THE FIRST ELEMENT OF THE ARRAY IS GREATER THAN T,
!   INTERCHANGE IT WITH T

indx = ind(i)
IF (x(indx) <= t) GO TO 50
ind(ij) = indx
ind(i) = it
it = indx
t = x(it)
GO TO 50

! INTERCHANGE ELEMENTS K AND L

40 itt = ind(l)
ind(l) = ind(k)
ind(k) = itt

! FIND AN ELEMENT IN THE UPPER PART OF THE ARRAY WHICH IS
!   NOT LARGER THAN T

50 l = l - 1
indx = ind(l)
IF (x(indx) > t) GO TO 50

! FIND AN ELEMENT IN THE LOWER PART OF THE ARRAY WHCIH IS NOT SMALLER THAN T

60 k = k + 1
indx = ind(k)
IF (x(indx) < t) GO TO 60

! IF K <= L, INTERCHANGE ELEMENTS K AND L

IF (k <= l) GO TO 40

! SAVE THE UPPER AND LOWER SUBSCRIPTS OF THE PORTION OF THE
!   ARRAY YET TO BE SORTED

IF (l-i > j-k) THEN
  il(m) = i
  iu(m) = l
  i = k
  m = m + 1
  GO TO 80
END IF

il(m) = k
iu(m) = j
j = l
m = m + 1
GO TO 80

! BEGIN AGAIN ON ANOTHER UNSORTED PORTION OF THE ARRAY

70 m = m - 1
IF (m == 0) RETURN
i = il(m)
j = iu(m)

80 IF (j-i >= 11) GO TO 30
IF (i == 1) GO TO 20
i = i - 1

! SORT ELEMENTS I+1,...,J.  NOTE THAT 1 <= I < J AND J-I < 11.

90 i = i + 1
IF (i == j) GO TO 70
indx = ind(i+1)
t = x(indx)
it = indx
indx = ind(i)
IF (x(indx) <= t) GO TO 90
k = i

100 ind(k+1) = ind(k)
k = k - 1
indx = ind(k)
IF (t < x(indx)) GO TO 100

ind(k+1) = it
GO TO 90
END SUBROUTINE quicksort
