set codedir=C:\Users\eric\git\ht
set distdir=C:\Users\eric\tomcat-ht\webapps\openads

copy /Y "%codedir%"\war\theme\* "%distdir%"\theme\
copy /Y "%codedir%"\war\scripts\* "%distdir%"\scripts\
copy /Y "%codedir%"\war\scripts\aperture\* "%distdir%"\scripts\aperture\
copy /Y "%codedir%"\war\scripts\xdataht\* "%distdir%"\scripts\xdataht\
copy /Y "%codedir%"\war\scripts\xdataht\modules\* "%distdir%"\scripts\xdataht\modules\
copy /Y "%codedir%"\war\scripts\xdataht\modules\graph\* "%distdir%"\scripts\xdataht\modules\graph\
copy /Y "%codedir%"\war\scripts\xdataht\modules\overview\* "%distdir%"\scripts\xdataht\modules\overview\
copy /Y "%codedir%"\war\scripts\xdataht\modules\util\* "%distdir%"\scripts\xdataht\modules\util\
copy /Y "%codedir%"\war\scripts\lib\* "%distdir%"\scripts\lib\
copy /Y "%codedir%"\war\*.html "%distdir%"\
copy /Y "%codedir%"\war\*.jsp "%distdir%"\
