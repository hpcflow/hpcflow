param($ExeName = "hpcflow", $BuildType = 'onedir')
Compress-Archive -Path ./dist/$BuildType/$ExeName -DestinationPath ./dist/$BuildType/$ExeName.zip
