(function(root) {
  root.ApplicationModel = function() {
    var zoomLevel;
    var selectedLayer = 'asset';
    return {
      moveMap: function(zoom, bbox) {
        var hasZoomLevelChanged = zoomLevel !== zoom;
        zoomLevel = zoom;
        eventbus.trigger('map:moved', {selectedLayer: selectedLayer, zoom: zoom, bbox: bbox, hasZoomLevelChanged: hasZoomLevelChanged});
      },
      setZoomLevel: function(level) {
        zoomLevel = level;
      },
      selectLayer: function(layer) {
        selectedLayer = layer;
        eventbus.trigger('layer:selected', layer);
      },
      getSelectedLayer: function() {
        return selectedLayer;
      },
      assetDragDelay: 100
    };
  };
})(this);
