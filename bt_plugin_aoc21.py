import bt2

bt2.register_plugin(__name__, "aoc21")


class AOC21SourceValuesIterator(bt2._UserMessageIterator):
    def __init__(self, config, output_port):
        self._values_file = output_port.user_data[0]
        self._ping_ec = output_port.user_data[1]

        ping_sc = self._ping_ec.stream_class
        tc = ping_sc.trace_class

        trace = tc()
        self._value_stream = trace.create_stream(ping_sc)

        self._stream_begin_msg = self._create_stream_beginning_message(
            self._value_stream
        )
        self._stream_end_msg = self._create_stream_end_message(self._value_stream)

    def __next__(self):
        if self._stream_begin_msg:
            msg = self._stream_begin_msg
            self._stream_begin_msg = None
            return msg

        try:
            line = next(self._values_file)
        except StopIteration:
            if self._stream_end_msg:
                msg = self._stream_end_msg
                self._stream_end_msg
                return msg
            else:
                raise StopIteration

        event_msg = self._create_event_message(
            self._ping_ec, self._value_stream, default_clock_snapshot=0
        )

        event_msg.event.payload_field["depth"] = int(line)
        return event_msg


@bt2.plugin_component_class
class AOC21Source(
    bt2._UserSourceComponent, message_iterator_class=AOC21SourceValuesIterator
):
    def __init__(self, config, params, obj):
        if "path" not in params:
            raise ValueError("AOC21Source: missing `path` parameter")

        path = params["path"]

        if type(path) != bt2._StringValueConst:
            raise TypeError(
                "AOC21Source: expecting `path` parameter to be a string, got a {}".format(
                    type(path)
                )
            )

        values_file = open(str(path), "r")

        tc = self._create_trace_class()
        cc = self._create_clock_class()
        sc = tc.create_stream_class(default_clock_class=cc)

        ping_event_payload = tc.create_structure_field_class()
        ping_event_payload.append_member(
            "depth", tc.create_unsigned_integer_field_class()
        )

        ping_event_ec = sc.create_event_class(
            name="ping", payload_field_class=ping_event_payload
        )

        self._add_output_port("the-values", (values_file, ping_event_ec))


@bt2.plugin_component_class
class AOC21Sink(bt2._UserSinkComponent):
    def __init__(self, config, params, obj):
        self._port = self._add_input_port("holiday-spirit")
        self._previous_depth = None
        self._increase_count = 0

    def _user_graph_is_configured(self):
        self._it = self._create_message_iterator(self._port)

    def _user_consume(self):
        msg = next(self._it)

        if type(msg) is bt2._EventMessageConst:
            depth = msg.event["depth"]
            if self._previous_depth and depth > self._previous_depth:
                self._increase_count = self._increase_count + 1
            self._previous_depth = depth
        elif type(msg) is bt2._StreamEndMessageConst:
            print(self._increase_count)
            raise bt2.Stop
