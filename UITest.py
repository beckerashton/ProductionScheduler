from nicegui import app, ui

app.native.window_args['resizable'] = True
app.native.start_args['debug'] = True
app.native.settings['ALLOW_DOWNLOADS'] = True

ui.label('Try this demo in native mode to see the events in action!')

app.native.on('minimized', lambda: print('Window minimized'))
app.native.on('resized', lambda e: print(f'{e.args["width"]}x{e.args["height"]}'))
app.native.on('drop', lambda e: print(f'Dropped files: {e.args["files"]}'))


ui.run(native=True, window_size=(400, 300), fullscreen=False)