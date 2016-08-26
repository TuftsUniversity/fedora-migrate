require 'open-uri'
require 'uri'
require 'curation_concerns'

module FedoraMigrate
  class VotingRecordTargetConstructor
    attr_accessor :source, :candidates, :target

    def initialize(source)
      @source = source
    end

    # Override .build to use Fedora's default id minter
    # https://github.com/projecthydra-labs/fedora-migrate/issues/49
    #obj.id FedoraMigrate::Mover.id_component(source)
    def build
      raise FedoraMigrate::Errors::MigrationError, "No qualified targets found in #{source.pid}" if target.nil?
      obj = target.new
      obj.apply_depositor_metadata "ebeck01"

      file_set = FileSet.new
      file_set.apply_depositor_metadata "ebeck01"

      voting_record = File.new "record.xml", "w" #File.basename(uri.path)
      voting_record.write source.datastreams['RECORD-XML'].content
      voting_record
      voting_record = File.new "record.xml"

      #fcrepo3_set = Fcrepo3FileSet.new
      ##fcrepo3_set.apply_depositor_metadata "mkorcy01"
      #fcrepo3_set.title = ['Fedora 3 Datastreams']
      #fcrepo3_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE
      #fcrepo3_set.thumbnail_id = nil

      #user = User.find_by_user_key(fcrepo3_set.depositor)
      #ds_actor = CurationConcerns::Actors::FileSetActor.new(fcrepo3_set, user)

    #  ['RELS-EXT','DC','DCA-ADMIN','DCA-META','DC-DETAIL-META'].each do |ds|
    ##    next if source.datastreams[ds].content.nil?
#        File.open(ds+".xml", 'w') {|f| f.write(source.datastreams[ds].content) }

#        ds_file = File.open(ds+".xml",'r:UTF-8')
 #       ds_actor.create_metadata(obj, {visibility: Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE})
  #      ds_actor.create_content(File.open(ds_file),'fcrepo3stream')
   #   end

      file_set.title = [source.datastreams['RECORD-XML'].label]
      file_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
      user = User.find_by_user_key(file_set.depositor)
      actor = CurationConcerns::Actors::FileSetActor.new(file_set, user)
      actor.create_metadata(obj, {visibility: Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC})
      actor.create_content(voting_record)


      obj.title = process_metadata_field 'dc:title'
      obj.creator = process_metadata_field 'dc:creator'
      obj.contributor = process_metadata_field 'dc:contributor'
      obj.description = process_metadata_field 'dc:description'
      obj.publisher = process_metadata_field 'dc:publisher'
      obj.source = process_metadata_field 'dc:source'
      obj.language = process_metadata_field 'dc:language'
      obj.f3_pid = source.pid
      obj.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
      obj


      # For now we're scrapping the fedora 3 id in the move but should be addressed.
    end

    def process_metadata_field field_name
      xml = Nokogiri::XML(source.datastreams["DCA-META"].content)
      field_values =  xml.xpath("//#{field_name}")
      target_values = Array.new
      field_values.each do |field|
        (target_values << field.text) unless field.nil?
      end

      target_values
    end

    def target
      @target ||= determine_target
    end

    private

      def determine_target

        Array(candidates).map { |model| vet(model) }.compact.first
      end

      def vet(model)
        # MK
        #print "\n\nVet Model\n\n"
        ##puts "MODEL #{model}"
        #puts "=END="

        FedoraMigrate::Mover.id_component(model).constantize
      rescue NameError
        Logger.debug "rejecting #{model} for target"
        nil
      end

    def candidates
      @candidates = Array(source.models).map do |candidate|
        if candidate == "info:fedora/cm:VotingRecord"
          "TuftsVotingRecord"
        else
          candidate
        end
      end
    end
  end



end

