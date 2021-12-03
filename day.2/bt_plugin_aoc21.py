import bt2

bt2.register_plugin(__name__, "aoc21")


class AOC21SourceCommandsIterator(bt2._UserMessageIterator):
    def __init__(self, config, output_port):
        self._commands_file = output_port.user_data[0]
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
            line = next(self._commands_file)
        except StopIteration:
            if self._stream_end_msg:
                msg = self._stream_end_msg
                self._stream_end_msg = None
                return msg
            else:
                raise StopIteration

        event_msg = self._create_event_message(
            self._ping_ec, self._value_stream, default_clock_snapshot=0
        )

        direction, distance = line.split(" ")
        distance = int(distance)

        event_msg.event.payload_field["direction"] = next(
            iter(event_msg.event.payload_field["direction"].cls[direction].ranges)
        ).lower
        event_msg.event.payload_field["distance"] = distance
        return event_msg


@bt2.plugin_component_class
class AOC21Source(
    bt2._UserSourceComponent, message_iterator_class=AOC21SourceCommandsIterator
):
    def __init__(self, config, params, obj):
        if "inputs" not in params:
            raise ValueError("{}: missing `inputs` parameter".format(type(self)))

        if type(params["inputs"]) is not bt2._ArrayValueConst:
            raise TypeError(
                "{}: unexpected `inputs` parameter type, got a {}".format(
                    type(self), type(params["inputs"])
                )
            )

        input_path = params["inputs"][0]

        if type(input_path) != bt2._StringValueConst:
            raise TypeError(
                "AOC21Source: expecting `inputs[0]` parameter to be a string, got a {}".format(
                    type(input_path)
                )
            )

        values_file = open(str(input_path), "r")

        tc = self._create_trace_class()
        cc = self._create_clock_class()
        sc = tc.create_stream_class(default_clock_class=cc)

        command_event_payload = tc.create_structure_field_class()

        direction_field_class = tc.create_unsigned_enumeration_field_class()
        direction_field_class.add_mapping("forward", bt2.UnsignedIntegerRangeSet([(0)]))
        direction_field_class.add_mapping("up", bt2.UnsignedIntegerRangeSet([(1)]))
        direction_field_class.add_mapping("down", bt2.UnsignedIntegerRangeSet([(2)]))

        command_event_payload.append_member("direction", direction_field_class)

        command_event_payload.append_member(
            "distance", tc.create_unsigned_integer_field_class()
        )

        ping_event_ec = sc.create_event_class(
            name="command", payload_field_class=command_event_payload
        )

        self._add_output_port("the-commands", (values_file, ping_event_ec))

    @staticmethod
    def _user_query(query_executor, obj, params, log_level):
        if obj == "babeltrace.support-info":
            if params["type"] == "file" and str(params["input"]).endswith(".aoc21"):
                w = 0.25
            else:
                w = 0.0

            return {"weight": w}
        else:
            raise bt2.UnknownObject


class Position:
    def __init__(self):
        self._horizontal = self._depth = self._aim = 0

    @property
    def horizontal(self) -> int:
        return self._horizontal

    @horizontal.setter
    def horizontal(self, val: int):
        self._horizontal = val

    @property
    def depth(self) -> int:
        return self._depth

    @depth.setter
    def depth(self, val: int):
        self._depth = val


class AOC21FilterPositionsIterator(bt2._UserMessageIterator):
    def __init__(self, config, output_port):
        self._position = Position()
        self._aim = 0

        input_port_name = next(iter(self._component._input_ports))
        input_port = self._component._input_ports[input_port_name]

        self._upstream_it = self._create_message_iterator(input_port)

        tc = self._component._create_trace_class()
        cc = self._component._create_clock_class()
        sc = tc.create_stream_class(default_clock_class=cc)

        position_event_payload = tc.create_structure_field_class()
        position_event_payload.append_member(
            "horizontal", tc.create_signed_integer_field_class()
        )
        position_event_payload.append_member(
            "depth", tc.create_signed_integer_field_class()
        )

        self._new_postion_event_ec = sc.create_event_class(
            name="new_position", payload_field_class=position_event_payload
        )

        trace = tc()
        self._new_positions_stream = trace.create_stream(sc)

        self._stream_begin_msg = self._create_stream_beginning_message(
            self._new_positions_stream
        )
        self._stream_end_msg = self._create_stream_end_message(
            self._new_positions_stream
        )

    def __next__(self):
        if self._stream_begin_msg:
            msg = self._stream_begin_msg
            self._stream_begin_msg = None
            return msg
        if self._stream_end_msg is None:
            raise StopIteration

        try:
            while True:
                msg = next(self._upstream_it)
                if type(msg) is bt2._EventMessageConst and msg.event.name == "command":
                    break
        except StopIteration as e:
            if self._stream_end_msg:
                msg = self._stream_end_msg
                self._stream_end_msg = None
                return msg

        direction = str(msg.event["direction"].labels[0])
        distance = msg.event["distance"]
        if direction == "forward":
            self._position.horizontal = self._position.horizontal + distance
            self._position.depth = self._position.depth + (self._aim * distance)
        elif direction == "up":
            self._aim = self._aim - distance
        elif direction == "down":
            self._aim = self._aim + distance

        event_msg = self._create_event_message(
            self._new_postion_event_ec,
            self._new_positions_stream,
            default_clock_snapshot=0,
        )
        event_msg.event.payload_field["horizontal"] = self._position.horizontal
        event_msg.event.payload_field["depth"] = self._position.depth

        return event_msg


@bt2.plugin_component_class
class AOC21Filter(
    bt2._UserFilterComponent, message_iterator_class=AOC21FilterPositionsIterator
):
    def __init__(self, config, params, obj):
        self._add_input_port("commands-in")
        self._add_output_port("positions-out")


@bt2.plugin_component_class
class AOC21Sink(bt2._UserSinkComponent):
    def __init__(self, config, params, obj):
        self._port = self._add_input_port("positions-in")
        self._position = Position()

    def _user_graph_is_configured(self):
        self._it = self._create_message_iterator(self._port)

    def _user_consume(self):
        msg = next(self._it)

        if type(msg) is bt2._EventMessageConst and msg.event.name == "new_position":
            self._position.horizontal = msg.event["horizontal"]
            self._position.depth = msg.event["depth"]

        elif type(msg) is bt2._StreamEndMessageConst:
            print(self._position.horizontal * self._position.depth)
            raise bt2.Stop
